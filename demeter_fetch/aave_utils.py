from _decimal import Decimal

RAY = 10**27


def decode_event_ReserveDataUpdated(row):
    return (
        Decimal(int(row["DATA"][2 : (2 + 64)], 16)) / RAY,
        Decimal(int(row["DATA"][(2 + 64) : (2 + 64 * 2)], 16)) / RAY,
        Decimal(int(row["DATA"][(2 + 64 * 2) : (2 + 64 * 3)], 16)) / RAY,
        Decimal(int(row["DATA"][(2 + 64 * 3) : (2 + 64 * 4)], 16)) / RAY,
        Decimal(int(row["DATA"][(2 + 64 * 4) :], 16)) / RAY,
    )
