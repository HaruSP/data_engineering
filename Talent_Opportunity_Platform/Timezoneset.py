from pytz import timezone
from datetime import datetime, timedelta
date = datetime.now(timezone('Asia/Seoul'))
date = date.strftime('%Y%m%d')
print(date)