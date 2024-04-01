import pandas as pd
import asyncio
import aiohttp
import requests


async def main():
    instrument_df = fetch_instruments()
    instrument_df['month_year'] = instrument_df['instrument_name'].apply(lambda x: x.split('-')[1][-5:])
    grouped = instrument_df.groupby('month_year')

    semaphore = asyncio.Semaphore(20)
    tasks = []
    for date, group_df in grouped:
        task = asyncio.create_task(gather_trades(group_df, date, semaphore))
        tasks.append(task)

    await asyncio.gather(*tasks)
        
def fetch_instruments():
    url = 'https://history.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option&include_old=true&expired=true'

    response = requests.get(url)
    data = response.json()['result']

    instrument_names = [item['instrument_name'] for item in data]
    instrument_df = pd.DataFrame(instrument_names, columns=['instrument_name'])
    return instrument_df

async def gather_trades(instrument_df, date, semaphore):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for instrument_name in instrument_df['instrument_name']:
            task = asyncio.create_task(fetch_trades(instrument_name, session, semaphore))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        all_trades = [trade for sublist in results for trade in sublist]
        all_trades_df = pd.DataFrame(all_trades)
        all_trades_df.to_parquet(f'options/{date}.parquet', index=False, compression='gzip')

    return
            
async def fetch_trades(instrument_name, session, semaphore):
    async with semaphore:
        print(f"Checking instrument: {instrument_name}")
        start_seq = 0
        has_more = True
        all_trades = []

        while has_more:
            trades, has_more = await request_trades(instrument_name, start_seq, session)
            start_seq += 10000
            all_trades.extend(trades)
            await asyncio.sleep(2)
         
        return all_trades

async def request_trades(instrument_name, start_seq, session):
    api_url = f"https://history.deribit.com/api/v2/public/get_last_trades_by_instrument?instrument_name={instrument_name}&count=10000&include_old=true&start_seq={start_seq}&sorting=asc"
    async with session.get(api_url) as response:
        if not response.status == 200:
            raise Exception(f"Request failed with status code {response.status}")
        
        data = await response.json()
        trades = data.get('result', {}).get('trades', [])
        has_more = data.get('result', {}).get('has_more', False)
        return trades, has_more


if __name__ == "__main__":
    asyncio.run(main())
