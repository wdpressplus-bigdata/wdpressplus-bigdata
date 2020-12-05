import pathlib
import requests

def download_file(filename):
    prefix = 'https://github.com/wdpressplus-bigdata/uscrn/raw/main'
    # prefix = 'https://www1.ncdc.noaa.gov/pub/data/uscrn/products/subhourly01'
    r = requests.get(f"{prefix}/2020/{filename}")
    r.raise_for_status()
    path = pathlib.Path('./raw')
    path.mkdir(parents=True, exist_ok=True)
    with open(path / filename, 'wb') as f:
        f.write(r.content)
    print(f"Saved {path / filename}")

FILES = [
    'CRNS0101-05-2020-AK_Aleknagik_1_NNE.txt',
    'CRNS0101-05-2020-AK_Bethel_87_WNW.txt',
]
for filename in FILES:
    download_file(filename)
