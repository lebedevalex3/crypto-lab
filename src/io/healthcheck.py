import pandas as pd
from loguru import logger


def ping() -> bool:
    df = pd.DataFrame({"ok": [True]})
    logger.info("Pandas ok, shape={}", df.shape)
    return True


ping()
