from parser import parse
from db import check_db, update_db
url = 'https://www.myhome.ge/en/s/iyideba-bina-Tbilisi/?Keyword=Vake-Saburtalo&AdTypeID=1&PrTypeID=1&cities=1&districts=111.28.30.38.39.40.41.42.43.44.45.46.47.101&regions=4&CardView=2&FCurrencyID=1&FPriceFrom=50000&FPriceTo=120000&AreaSizeID=1&AreaSizeFrom=30&AreaSizeTo=60&RoomNums=1.2'

data = parse(url)
update_db(data)
# check_db()
