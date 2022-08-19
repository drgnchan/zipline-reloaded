import pandas as pd

from zipline.data.bundles import register
from zipline.data.bundles.csvdir import csvdir_equities


start_session = pd.Timestamp('2016-1-1', tz='utc')
end_session = pd.Timestamp('2018-1-1', tz='utc')
register(
    'custom-csvdir-bundle',
    csvdir_equities(
        ['daily'],
        '/path/to/your/csvs',
    ),
    calendar_name='NYSE', # US equities
    start_session=start_session,
    end_session=end_session
)