import pandas as pd
from pathlib import Path
from datetime import datetime
import yaml
from usagrid import eia

def main():

    with open("cred.yaml") as f:

        data = yaml.safe_load(f.read())

        api_key = data["EIA"]["key"]

        
    start_date = "2022-01-01"
    end_date = "2024-03-06"

    EIA_mod = eia.EIA(api_key=api_key,
                start_date=start_date,
                end_date=end_date)

    df = EIA_mod.get_balancingauthority(test=False)


if __name__ == '__main__':
    main()

