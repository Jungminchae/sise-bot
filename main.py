import time
from datetime import datetime
from helium import *
from enums import SiseLink
from parsing import ETFSiseExtractor


def select_eft_type_tab(driver, name):
    wait_until(Link(name).exists)
    if Link(name).exists():
        click(Link(name))
        time.sleep(0.5)
        sourse = driver.page_source
        extractor = ETFSiseExtractor(sourse)

        today = datetime.today().strftime("%Y%m%d")
        df = extractor.extract_table()
        if "/" in name:
            name = name.replace("/", ".")
        df.write_csv(f"{name}_{today}.csv", include_header=True)


def main(driver):
    select_eft_type_tab(driver, "국내 시장지수")
    select_eft_type_tab(driver, "국내 업종/테마")


if __name__ == "__main__":
    driver = start_chrome(SiseLink.ETF, maximize=True, headless=False)
    main(driver)
    kill_browser()
