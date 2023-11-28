from pyspark.sql import SparkSession
from geopy.geocoders import Nominatim

spark = SparkSession.builder.getOrCreate()

def geocoding(address):
    geo_local = Nominatim(user_agent='South Korea')
    try:
        geo = geo_local.geocode(address)
        x_y = [geo.latitude, geo.longitude]
        return x_y
    except:
        return [0, 0]

# JSON 데이터를 DataFrame으로 변환
def convert_json_to_dataframe(json_data):
    metainfo = json_data["categories"][0]["metainfo"]
    location1 = metainfo["location1"]
    location2 = metainfo["location2"]
    Type1 = metainfo["Type1"]
    Type2 = metainfo["Type2"]
    name_kr = metainfo["name_kr"]
    location = metainfo["add"]
    lat, lng = geocoding(location)
    
    # DataFrame 생성
    data = [(location1, location2, Type1, Type2, name_kr, location, lat, lng)]
    columns = ['location1', 'location2', 'Type1', 'Type2', 'name_kr', 'location', 'lat', 'lng']
    df = spark.createDataFrame(data, columns)
    return df

if __name__ == "__main__":
    csv_file_path = 'landmarks.csv'
    folder_path = "./라벨링데이터/서울특별시"
    
    df = spark.createDataFrame([], ['location1', 'location2', 'Type1', 'Type2', 'name_kr', 'location', 'lat', 'lng'])
    
    folder_list = os.listdir(folder_path)
    for landmark in folder_list:
        for root, dirs, files in os.walk(f"{folder_path}/{landmark}"):
            file_name = files[0]
            if file_name.endswith('.json'):
                json_file_path = os.path.join(root, file_name)
                # JSON 파일 읽기
                with open(json_file_path, 'r', encoding='utf-8') as json_file:
                    json_data = json.load(json_file)
                    json_df = convert_json_to_dataframe(json_data)
                    df = df.union(json_df)
    
    df.write.mode("append").csv(csv_file_path, header=True)
