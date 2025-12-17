import asyncio
from dataclasses import dataclass
from enum import Enum, unique
from datetime import datetime

from coag import CoagTest, DepContainer, TestResult

encoding = "cp1251"

@unique
class SpChar(Enum):
    STX = 0x02
    ETX = 0x03
    EOT = 0x04
    ENQ = 0x05
    ACK = 0x06
    LF  = 0x0a
    CR  = 0x0d
    NAK = 0x15

    def to_int(self):
        return self.value
    
    def to_bytes(self):
        return bytes([self.value])
    
    def __str__(self) -> str:
        return f"<{self.name}>"

lis_parameters: dict[CoagTest, dict[str, tuple[str, str, str]]] = {
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

@dataclass
class LisResult:
    result_id: str
    result_value: str
    result_units: str = ""

def lis_format_parameter_value(test_type: CoagTest, parameter_name: str, parameter_value: str) -> LisResult:
    test_parameters = lis_parameters.get(test_type, {})
    par_fmt = test_parameters.get(parameter_name, None)
    if par_fmt:
        (res_id, format_string, units) = par_fmt
        return LisResult(res_id, format_string.format(parameter_value), units)
    return None

def frame_to_str(frame: bytearray) -> str:
    sp_char_map = {c.to_int(): str(c) for c in SpChar}
    chars = []
    for b in frame:
        ch = sp_char_map.get(b, None) or bytes([b]).decode(encoding)
        chars.append(ch)
    return ''.join(chars)

class DataLinkState(Enum):
    NEUTRAL = 0
    CONNECTED = 1
    TRANSFER = 2

class ASTM:
    def __init__(self, host: str = None, port: int = None, encoding: str = None) -> None:
        self.host = host or "localhost"
        self.port = port or 8888
        self.encoding = encoding or "utf-8"

        self.state: DataLinkState = DataLinkState.NEUTRAL
        self.frame_number = 0
        self.reader = None
        self.writer = None

    async def connect(self):
        success: bool = False
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            print(f"LIS DEBUG established connection to {self.host}:{self.port}")
            self.state = DataLinkState.CONNECTED

            self.writer.write(SpChar.ENQ.to_bytes())
            await self.writer.drain()
            print("LIS DEBUG sent ENQ")
            resp = await asyncio.wait_for(self.reader.read(255), 15)
            print(f"LIS DEBUG received: {resp}")
            success = len(resp) > 0 and resp[0] == SpChar.ACK.value
            if success:
                self.state = DataLinkState.TRANSFER
                self.frame_number = 1
        except asyncio.TimeoutError:
            print(f"LIS DEBUG establish phase timeout")
        
        return success

    async def send_header(self, device_name: str, device_version: str, device_id: str):
        if self.state == DataLinkState.TRANSFER:
            return await self._send_frame('H', f"|\\^&|||{device_name}^{device_version}^{device_id}|||||||P|E1394-97".encode(self.encoding))

    async def send_order(self, sample_id: str, date_time: str):
        if self.state == DataLinkState.TRANSFER:
            return await self._send_frame('O', f"|1|{sample_id}|||||{date_time}".encode(self.encoding))

    async def send_result(self, seq_number: int, result_id: str, result_value: str, units: str) -> bytearray:
        if self.state == DataLinkState.TRANSFER:
            return await self._send_frame('R', f"|{seq_number}|^^^{result_id}|{result_value}|{units}".encode(self.encoding))
    
    async def send_terminator(self) -> bytearray:
        if self.state == DataLinkState.TRANSFER:
            return await self._send_frame('L', "|1|N".encode(self.encoding))

    async def disconnect(self):
        prev_state = self.state
        self.state = DataLinkState.NEUTRAL

        if prev_state in [DataLinkState.TRANSFER, DataLinkState.CONNECTED]:
            self.writer.write(SpChar.EOT.to_bytes())
            await self.writer.drain()
            print("LIS DEBUG! sent EOT")
            if prev_state == DataLinkState.TRANSFER:
                prev_state = DataLinkState.CONNECTED

        if prev_state == DataLinkState.CONNECTED:
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None
            self.reader = None
            print('LIS DEBUG! connection closed')

    async def _send_frame(self, record_type: str, payload: bytes):
        #form frame
        frame = bytearray()
        checksum = self._calc_checksum(self.frame_number, payload)
        frame.extend(SpChar.STX.to_bytes())
        frame.extend(str(self.frame_number).encode(self.encoding))
        frame.extend(record_type.encode(self.encoding))
        frame.extend(payload)
        frame.extend(SpChar.CR.to_bytes())
        frame.extend(SpChar.ETX.to_bytes())
        frame.extend(format(checksum, '02X').encode(self.encoding))
        frame.extend(SpChar.CR.to_bytes())
        frame.extend(SpChar.LF.to_bytes())

        #try to send frame
        try:
            self.writer.write(frame)
            await self.writer.drain()
            print(f"LIS DEBUG sent frame\n  bytes: {frame}\n  str: {frame_to_str(frame)}")

            resp = await asyncio.wait_for(self.reader.read(255), 15)
            print(f"LIS DEBUG received response: {resp}")
            if len(resp) > 0 and resp[0] == SpChar.ACK.value:
                self._increment_frame_number()
                return True
            else:
                print(f"LIS DEBUG received NAK in response")
                self._increment_frame_number()
            return False
        except asyncio.TimeoutError:
            print(f"LIS DEBUG transfer phase timeout")
            return False

    def _increment_frame_number(self):
        self.frame_number = (self.frame_number + 1) % 8

    def _calc_checksum(self, frame_number: int, data: bytes) -> int:
        sum = 0
        for x in str(frame_number).encode(self.encoding):
            sum += x
        for x in data:
            sum += x
        return sum & 0xff

@dataclass
class LisPackage:
    sample_id: str
    test_type: CoagTest
    results: list[TestResult]
    dt: datetime

class LisError(Exception):
    pass

async def send_results_to_lis_loop(deps: DepContainer):    
    print("LIS DEBUG result sending task started")

    settings = deps.settings
    lis_queue: asyncio.Queue = deps.lis_queue
    astm: ASTM = None

    while True:
        try:
            lp: LisPackage = await lis_queue.get()

            (host, port, *_) = settings.lis_address.split(":")
            if not astm or astm.host != host or astm.port != port:
                astm = ASTM(host=host, port=port, encoding=encoding)

            connected = await astm.connect()
            if connected:
                try:
                    if not await astm.send_header("КоаТест-Авто", deps.version, deps.settings.device_id):
                        raise LisError

                    dt_str = lp.dt.strftime("%Y%m%d%H%M%S")
                    if not await astm.send_order(lp.sample_id, dt_str):
                        raise LisError

                    res_num = 1
                    for tr in lp.results:
                        lis_result = lis_format_parameter_value(lp.test_type, tr.parameter, tr.value)
                        if lis_result:
                            if not await astm.send_result(res_num, lis_result.result_id, lis_result.result_value, lis_result.result_units):
                                raise LisError
                            res_num += 1

                    await astm.send_terminator()
                except LisError:
                    pass

            await astm.disconnect()
        except Exception as e:
            print("LIS DEBUG ERROR:")
            print(e)

