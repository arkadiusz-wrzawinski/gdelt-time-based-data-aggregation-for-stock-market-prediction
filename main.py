import pandas as pd
import asyncio

from colorama import Fore, Style

from masterlist import download_masterlist, process_masterlist, split_masterlist, get_years, split_into_years
from downloading import download_all

async def main():
    cache: bool = True
    total_stages: int = 3
    years_to_process: list[int] = [2021, 2022, 2023, 2024, 2025]

    print("Processing data")
    print("Using cache" if cache else "Not using cache")
    print(f"Total stages: {total_stages}")
    print("")

    masterlist = download_masterlist(cache=cache)
    print(Fore.GREEN + f"Progress 1/{total_stages}" + Style.RESET_ALL)
    print("Finished downloading masterlist")
    print("Total files:", len(masterlist))
    print("")

    masterlist = process_masterlist(masterlist, cache=cache)
    print(Fore.GREEN + f"Progress 2/{total_stages}" + Style.RESET_ALL)
    print("Finished processing masterlist")
    print("")

    events, mentions, details = split_masterlist(masterlist, cache=cache)
    print(Fore.GREEN + f"Progress 3/{total_stages}" + Style.RESET_ALL)
    print("Finished splitting masterlist")
    print("Number of event files:", len(events))
    print("Number of mention files:", len(mentions))
    print("Number of detail files:", len(details))
    print("")

    years = get_years(masterlist)
    print(Fore.GREEN + f"Progress 4/{total_stages}" + Style.RESET_ALL)
    print("Extracted years from masterlist")
    print("Number of years:", len(years))
    print("")

    events = split_into_years(events, years)
    mentions = split_into_years(mentions, years)
    details = split_into_years(details, years)
    print(Fore.GREEN + f"Progress 5/{total_stages}" + Style.RESET_ALL)
    print("Finished splitting masterlist into years")
    print("")

    for year in years_to_process:
        print(f"Downloading events for year {year}")
        await download_all(events[year], "event", cache=cache)
        print("")

        print(f"Downloading details for year {year}")
        await download_all(details[year], "detail", cache=cache)
        print("")

        print(f"Downloading mentions for year {year}")
        await download_all(mentions[year], "mention", cache=cache)
        print("")

if __name__ == "__main__":
    asyncio.run(main())