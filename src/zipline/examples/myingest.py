import zipline.data.bundles as b
import tushare_source as ts
import exchange_calendars as xcals
import pandas as pd


result = xcals.get_calendar_names(include_aliases=False)[5:10]
print(result)
bundle = 'a_stock'
b.register(
    bundle,
    ts.tushare_bundle,
    calendar_name='XSHG',
    start_session=pd.Timestamp("2001-05-22")
)

b.ingest(bundle)
