import logging


def configure_logging(debug: bool = False) -> None:
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
