from typing import NamedTuple

class BoundingBox(NamedTuple):
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    name: str


class VPS:
    lat = 48.427455
    lon = -126.174124


class BCFTerminal:
    class Tsawwassen:
        lat: float = 49.006621
        lon: float = -123.132309

    class DukePoint:
        lat: float = 49.162529
        lon: float = -123.891036

    class DepartureBay:
        lat: float = 49.193512
        lon: float = -123.954777

    class Gabriola:
        lat: float = 49.177846
        lon: float = -123.858655

    class NanaimoHarbor:
        lat: float = 49.166714
        lon: float = -123.930933

    class SwartzBay:
        lat: float = 48.689047
        lon: float = -123.410817

    class PortMcneil:
        lat: float = 50.592621
        lon: float = -127.085620

    class AlertBay:
        lat: float = 50.587972
        lon: float = -126.931313

    class Sointula:
        lat: float = 50.626701
        lon: float = -127.018700

    class HorseshoeBay:
        lat: float = 49.375791
        lon: float = -123.271643


TsawwassenBox = BoundingBox(lat_min=BCFTerminal.Tsawwassen.lat - 0.05,
                            lat_max=BCFTerminal.Tsawwassen.lat + 0.075,
                            lon_min=BCFTerminal.Tsawwassen.lon - 0.15,
                            lon_max=BCFTerminal.Tsawwassen.lon + 0.075,
                            name='Tsawwassen')

DukePointBox = BoundingBox(lat_min=BCFTerminal.DukePoint.lat - 0.018,
                           lat_max=BCFTerminal.DukePoint.lat + 0.03,
                           lon_min=BCFTerminal.DukePoint.lon - 0.01,
                           lon_max=BCFTerminal.DukePoint.lon + 0.018,
                           name='DukePoint')


DepartureBayBox = BoundingBox(lat_min=BCFTerminal.DepartureBay.lat - 0.01,
                              lat_max=BCFTerminal.DepartureBay.lat + 0.03,
                              lon_min=BCFTerminal.DepartureBay.lon - 0.03,
                              lon_max=BCFTerminal.DepartureBay.lon + 0.025,
                              name='DepartureBay')

GabriolaBox = BoundingBox(lat_min=BCFTerminal.Gabriola.lat - 0.01,
                          lat_max=BCFTerminal.Gabriola.lat + 0.01,
                          lon_min=BCFTerminal.Gabriola.lon - 0.01,
                          lon_max=BCFTerminal.Gabriola.lon + 0.01,
                          name='Gabriola')

NanaimoHarborBox = BoundingBox(lat_min=BCFTerminal.NanaimoHarbor.lat - 0.01,
                               lat_max=BCFTerminal.NanaimoHarbor.lat + 0.01,
                               lon_min=BCFTerminal.NanaimoHarbor.lon - 0.01,
                               lon_max=BCFTerminal.NanaimoHarbor.lon + 0.01,
                               name='NanaimoHarbor')

SwartzBayBox = BoundingBox(lat_min=BCFTerminal.SwartzBay.lat - 0.02,
                           lat_max=BCFTerminal.SwartzBay.lat + 0.02,
                           lon_min=BCFTerminal.SwartzBay.lon - 0.02,
                           lon_max=BCFTerminal.SwartzBay.lon + 0.02,
                           name='SwartzBay')

PortMcneilBox = BoundingBox(lat_min=BCFTerminal.PortMcneil.lat - 0.015,
                            lat_max=BCFTerminal.PortMcneil.lat + 0.015,
                            lon_min=BCFTerminal.PortMcneil.lon - 0.015,
                            lon_max=BCFTerminal.PortMcneil.lon + 0.015,
                            name='PortMcNeil')

AlertBayBox = BoundingBox(lat_min=BCFTerminal.AlertBay.lat - 0.015,
                          lat_max=BCFTerminal.AlertBay.lat + 0.015,
                          lon_min=BCFTerminal.AlertBay.lon - 0.015,
                          lon_max=BCFTerminal.AlertBay.lon + 0.015,
                          name='AlertBay')


SointulaBox = BoundingBox(lat_min=BCFTerminal.Sointula.lat - 0.015,
                          lat_max=BCFTerminal.Sointula.lat + 0.015,
                          lon_min=BCFTerminal.Sointula.lon - 0.015,
                          lon_max=BCFTerminal.Sointula.lon + 0.015,
                          name='Sointula')


HorseshoeBayBox = BoundingBox(lat_min=BCFTerminal.HorseshoeBay.lat - 0.045,
                              lat_max=BCFTerminal.HorseshoeBay.lat + 0.045,
                              lon_min=BCFTerminal.HorseshoeBay.lon - 0.045,
                              lon_max=BCFTerminal.HorseshoeBay.lon + 0.055,
                              name='HorseshoeBay')

# For older ferry transits, data may stop before entering any of the Nanaimo terminal bounding boxes,
# so this box is used to estimate a larger area for Nanaimo departure or arrival sailings.
NanaimoAreaBox = BoundingBox(lat_min = 49.13699 - 0.075,
                             lat_max = 49.25 + 0.075,
                             lon_min = -123.985 - 0.15,
                             lon_max = -123.775 + 0.075,
                             name = 'GreaterNanaimoArea')
