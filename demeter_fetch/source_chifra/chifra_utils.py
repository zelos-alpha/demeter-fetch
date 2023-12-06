from datetime import date, datetime

from demeter_fetch import FromConfig, DappType, utils


def get_export_commend(from_config: FromConfig) -> str:
    match from_config.dapp_type:
        case DappType.uniswap:
            contract_addr = from_config.uniswap_config.pool_address
        case _:
            raise RuntimeError(f"Unsupported dapp type {from_config.dapp_type}")

    start_height = utils.ApiUtil.query_blockno_from_time(
        from_config.chain,
        datetime.combine(from_config.chifra_config.start, datetime.min.time()),
        False,
        from_config.http_proxy,
        from_config.chifra_config.etherscan_api_key,
    )
    end_height = utils.ApiUtil.query_blockno_from_time(
        from_config.chain,
        datetime.combine(from_config.chifra_config.end, datetime.max.time()),
        True,
        from_config.http_proxy,
        from_config.chifra_config.etherscan_api_key,
    )
    output_file_name = f"{from_config.chain.value}_{contract_addr}_height({start_height}-{end_height}).csv"
    return f"chifra export --logs --fmt csv --first_block {start_height} --last_block {end_height} {contract_addr} > {output_file_name}"
