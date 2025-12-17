import asyncio
from dataclasses import dataclass
from enum import Enum, unique
from datetime import datetime
from typing import Optional, List, Dict, Tuple

from coag import CoagTest, DepContainer, TestResult

DEFAULT_ENCODING = "cp1251"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8888
TIMEOUT_SECONDS = 15


class LisError(Exception):
    pass


class LisConnectionError(LisError):
    pass


class DataTransmissionError(LisError):
    pass


@unique
class SpecialChar(Enum):
    STX = 0x02
    ETX = 0x03
    EOT = 0x04
    ENQ = 0x05
    ACK = 0x06
    LF = 0x0A
    CR = 0x0D
    NAK = 0x15

    def to_int(self):
        return self.value

    def to_bytes(self):
        return bytes([self.value])

    def __str__(self) -> str:
        return f"<{self.name}>"


class DataLinkState(Enum):
    NEUTRAL = 0
    CONNECTED = 1
    TRANSFER = 2


class ASTMRecordType(Enum):
    HEADER = "H"
    ORDER = "O"
    RESULT = "R"
    TERMINATOR = "L"


@dataclass
class LisResult:
    result_id: str
    result_value: str
    result_units: str = ""


@dataclass
class LisPackage:
    sample_id: str
    test_type: CoagTest
    results: List[TestResult]
    dt: datetime


class ASTM:
    LIS_PARAMETRS: Dict[CoagTest, Dict[str, Tuple[str, str, str]]] = {
        CoagTest.PT: {
            "Время": ("PT", "{:.1f}", "с"),
            "ПТИ": ("PTI", "{:.0f}", ""),
            "МНО": ("INR", "{:.2f}", ""),
            "По Квику": ("PT_QUICK", "{:.1f}", "%"),
        },
        CoagTest.APTT: {
            "Время": ("APTT", "{:.1f}", "с"),
        },
        CoagTest.TT: {
            "Время": ("TT", "{:.1f}", "с"),
        },
        CoagTest.FIBR: {
            "Время": ("FIBR_T", "{:.1f}", "с"),
            "Конц.": ("FIBR_C", "{:.2f}", "г/л"),
        },
        CoagTest.DDIMER: {
            "Конц.": ("DDIMER", "{:.2f}", "нг/мл"),
        },
        CoagTest.FACTOR8: {
            "Активность": ("FVIII", "{:.1f}", "%"),
        },
        CoagTest.FACTOR9: {
            "Активность": ("FIX", "{:.1f}", "%"),
        },
        CoagTest.ANTITR: {
            "Активность": ("ATIII", "{:.1f}", "%"),
        },
    }

    def __init__(self, encoding: str = DEFAULT_ENCODING):
        self.encoding = encoding

    def lis_format_parameter(
        self, test_type: CoagTest, parameter_name: str, parameter_value: float
    ) -> Optional[LisResult]:
        test_parameters = self.LIS_PARAMETRS.get(test_type, {})
        parameter_format = test_parameters.get(parameter_name, None)
        if not parameter_format:
            raise LisError(f"Unknown LIS parameter format for: {test_type}")
        res_id, format_string, units = parameter_format
        return LisResult(res_id, format_string.format(parameter_value), units)

    def create_header(
        self, device_name: str, device_version: str, device_id: str
    ) -> bytes:
        header = f"|\\^&|||{device_name}^{device_version}^{device_id}|||||||P|E1394-97"
        return header.encode(self.encoding)

    def create_order(self, sample_id: str, date_time: datetime) -> bytes:
        date_str = date_time.strftime("%Y%m%d%H%M%S")
        order = f"|1|{sample_id}|||||{date_str}"
        return order.encode(self.encoding)

    def create_result(
        self, seq_number: int, result_id: str, result_value: str, units: str
    ) -> bytes:
        result = f"|{seq_number}|^^^{result_id}|{result_value}|{units}"
        return result.encode(self.encoding)

    def create_terminator(self) -> bytes:
        return b"|1|N"

    def build_frame(
        self, frame_number: int, record_type: ASTMRecordType, payload: bytes
    ) -> bytearray:
        # form frame
        frame = bytearray()
        frame.extend(SpecialChar.STX.to_bytes())
        frame.extend(str(frame_number).encode(self.encoding))
        frame.extend(record_type.value.encode(self.encoding))
        frame.extend(payload)
        frame.extend(SpecialChar.CR.to_bytes())
        frame.extend(SpecialChar.ETX.to_bytes())
        checksum = self._calculate_checksum(frame_number, payload)
        frame.extend(format(checksum, "02X").encode(self.encoding))
        frame.extend(SpecialChar.CR.to_bytes())
        frame.extend(SpecialChar.LF.to_bytes())

        return frame

    def frame_to_str(self, frame: bytearray) -> str:
        sp_char_map = {c.to_int(): str(c) for c in SpecialChar}
        chars = []
        for b in frame:
            ch = sp_char_map.get(b, None) or bytes([b]).decode(self.encoding)
            chars.append(ch)
        return "".join(chars)

    def _calculate_checksum(self, frame_number: int, data: bytes) -> int:
        checksum = 0
        checksum += sum(str(frame_number).encode(self.encoding))
        checksum += sum(data)

        return checksum & 0xFF


class ASTMConnection:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        astm: Optional[ASTM] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.astm = astm or ASTM()

        self.state: DataLinkState = DataLinkState.NEUTRAL
        self.frame_number = 0
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def __aenter__(self) -> "ASTMConnection":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> bool:
        if self.state != DataLinkState.NEUTRAL:
            raise LisConnectionError("Connection from not neutral state")

        print(f"LIS DEBUG connecting to LIS at {self.host}:{self.port}")
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=TIMEOUT_SECONDS
            )
            print(f"LIS DEBUG established connection to {self.host}:{self.port}")
            self.state = DataLinkState.CONNECTED

            self.writer.write(SpecialChar.ENQ.to_bytes())
            await self.writer.drain()
            print("LIS DEBUG sent ENQ")

            response = await self._receive_response()

            if response and response[0] == SpecialChar.ACK.value:
                self.state = DataLinkState.TRANSFER
                self.frame_number = 1
                print("LIS DEBUG Connection established successfully")
                return True
            else:
                print(f"LIS DEBUG unexpected response during connection: {response}")
                return False

        except asyncio.TimeoutError:
            print("LIS DEBUG establish connection phase timeout")
            return False
        except (LisConnectionError, OSError) as e:
            print(f"LIS DEBUG unexpected error during connection: {e}")
            return False

    async def send_package(
        self, package: LisPackage, device_info: Dict[str, str]
    ) -> bool:
        if self.state != DataLinkState.TRANSFER:
            print("LIS DEBUG connection not ready for data transfer")
            return False

        try:
            header_payload = self.astm.create_header(
                device_info.get("name", "КоаТест-Авто"),
                device_info.get("version", "UNKNOWN"),
                device_info.get("id", "UNKNOWN"),
            )

            if not await self._send_frame(ASTMRecordType.HEADER, header_payload):
                raise DataTransmissionError("Failed to send header")

            order_payload = self.astm.create_order(package.sample_id, package.dt)

            if not await self._send_frame(ASTMRecordType.ORDER, order_payload):
                raise DataTransmissionError("Failed to send order")

            sequence_num = 1
            for test_result in package.results:
                astm_result = self.astm.lis_format_parameter(
                    package.test_type, test_result.parameter, test_result.value
                )

                if astm_result:
                    result_payload = self.astm.create_result(
                        sequence_num,
                        astm_result.result_id,
                        astm_result.result_value,
                        astm_result.result_units,
                    )

                    if not await self._send_frame(
                        ASTMRecordType.RESULT, result_payload
                    ):
                        raise DataTransmissionError(
                            f"Failed to send result {sequence_num}"
                        )

                    sequence_num += 1

            terminator_payload = self.astm.create_terminator()
            if not await self._send_frame(
                ASTMRecordType.TERMINATOR, terminator_payload
            ):
                raise DataTransmissionError("Failed to send terminator")

            print(f"LIS DEBUG successfully sent package for sample {package.sample_id}")
            return True

        except Exception as e:
            raise DataTransmissionError(
                f"Unknown error occured while sending package: {e}"
            )
        finally:
            self.state = DataLinkState.CONNECTED

    async def disconnect(self) -> None:
        try:
            if self.state in [DataLinkState.CONNECTED, DataLinkState.TRANSFER]:
                self.writer.write(SpecialChar.EOT.to_bytes())
                await self.writer.drain()
                print("LIS DEBUG Sent EOT")

            await self._closing_writer()
            print("LIS DEBUG Disconnected from LIS")

        except Exception as e:
            print(f"LIS DEBUG error occuring dduring disconnect: {e}")
        finally:
            self.state = DataLinkState.NEUTRAL

    async def _closing_writer(self) -> None:
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except Exception as e:
            print(f"LIS DEBUG error occuring during closing writer: {e}")
        finally:
            self.reader = None
            self.writer = None

    async def _send_frame(self, record_type: ASTMRecordType, payload: bytes):
        if self.state != DataLinkState.TRANSFER:
            raise LisConnectionError(f"Cannot send frame from state {self.state}")
        # try to send frame
        try:
            frame = self.astm.build_frame(self.frame_number, record_type, payload)

            self.writer.write(frame)
            await self.writer.drain()

            print(
                f"LIS DEBUG sent frame\n  bytes: {frame}\n  str: {self.astm.frame_to_str(frame)}"
            )

            response = await self._receive_response()

            if response and response[0] == SpecialChar.ACK.value:
                self._increment_frame_number()
                print(f"LIS DEBUG responsed for frame {frame} with ACK")
                return True
            else:
                raise DataTransmissionError(f"Received NAK for frame {frame}")
        except asyncio.TimeoutError:
            raise LisConnectionError("Connection timeout")
        except Exception as e:
            raise LisError(f"Unknown error occured while sending frame: {e}")

    async def _receive_response(self) -> Optional[bytes]:
        if not self.reader:
            return None

        try:
            response = await asyncio.wait_for(
                self.reader.read(255), timeout=TIMEOUT_SECONDS
            )
            print(f"LIS DEBUG received response: {response}")
            return response
        except asyncio.TimeoutError:
            raise LisConnectionError("Connection timeout")

    def _increment_frame_number(self):
        self.frame_number = (self.frame_number + 1) % 8


class LisService:

    def __init__(self) -> None:
        self.astm: Optional[ASTM] = None

    async def send_results(self, deps: DepContainer, package: LisPackage) -> bool:
        try:
            host, port, *_ = deps.settings.lis_address.split(":")

            device_info = {
                "name": "КоаТест-Авто",
                "version": deps.version,
                "id": deps.settings.device_id,
            }

            async with ASTMConnection(host, int(port), self.astm) as connection:
                return await connection.send_package(package, device_info)

        except LisConnectionError as e:
            print(f"LIS DEBUG failed to establish connection: {e}")
            return False
        except Exception as e:
            print(f"LIS ERROR occured during sending results to LIS: {e}")
            return False


async def send_results_to_lis_worker(deps: DepContainer):
    print("LIS DEBUG result sending task started")

    lis_service = LisService()
    lis_queue: asyncio.Queue = deps.lis_queue

    while True:
        try:
            package: LisPackage = await lis_queue.get()
            print(f"LIS DEBUG proccesing for sample {package.sample_id}")

            success = await lis_service.send_results(deps, package)

            if success:
                print(
                    f"LIS DEBUG succesfully sent result for smaple {package.sample_id}"
                )
            else:
                print(
                    f"LIS DEBUG failted to send result for sample {package.sample_id}"
                )

        except Exception as e:
            print("LIS DEBUG ERROR:")
            print(e)

        finally:
            lis_queue.task_done()

