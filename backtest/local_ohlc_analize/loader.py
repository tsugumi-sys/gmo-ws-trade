from hydra.utils import to_absolute_path
import os
import subprocess

os.environ["HYDRA_FULL_ERROR"] = "1"


def gmo_data_loader(exchange_name: str, trading_type: str, pair_name: str) -> None:
    if exchange_name != "gmo":
        raise ValueError(f"Invalid exchange name: {exchange_name}.You should use 'gmo'.")

    execute_shell_script(exchange_name=exchange_name, trading_type=trading_type, pair_name=pair_name)


def execute_shell_script(exchange_name: str, trading_type: str, pair_name: str) -> None:
    shell_script_path: str = os.path.join(to_absolute_path("data_loader/gmo_data_loader"), "download_klines.sh")
    print(shell_script_path)
    subprocess.run(["chmod", "+x", shell_script_path])
    subprocess.run(
        [
            shell_script_path,
            exchange_name,
            trading_type,
            pair_name,
        ]
    )
