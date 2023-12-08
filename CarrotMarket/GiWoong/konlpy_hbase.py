from konlpy.tag import Okt
from pyspark import SparkContext
from pyspark.sql import SparkSession
import happybase

connection = happybase.Connection('localhost')

table_name = "test_table2"
table = connection.table(table_name)

article = """
카카오 벤티·타다는 설치 못한다…택시 강도 막는 '택시의 비밀' [강갑생의 바퀴와 날개]
중앙일보
입력 2023.11.17 06:00

업데이트 2023.11.17 11:04
 일반 승용차와 택시를 구별하는 방법은 몇 가지가 있습니다. 우선 번호판 바탕이 흰색인 일반 승용차와 달리 영업용인 택시는 노란색입니다. 또 멀리서도 식별이 가능한 수단이 하나 더 있는데요. 바로 지붕 위에 부착된 '택시 갓등' 입니다. 갓등은 전구나 불 위에 갓을 씌운 등을 통틀어 이르는 말인데요. 택시 갓등은 '택시 방범등' 또는 '택시 표시등'으로 부르기도 합니다.

 현행 여객자동차운수사업법 시행규칙엔 ‘택시운송사업용 자동차 윗부분에는 택시운송사업용 자동차임을 표시하는 설비를 설치하고, 빈 차로 운행 중일 때에는 외부에서 빈 차임을 알 수 있도록 조명장치가 자동으로 작동되는 설비를 갖춰야 한다’는 규정이 있습니다. 여기서 말하는 설비가 바로 택시 갓등입니다.

 택시업계에 따르면 택시는 영업구역이 정해져 있기 때문에 택시 갓등이 ‘우리 지역 택시’라고 알려주는 길잡이 역할도 하는데요. 그래서 구별을 위해 지역별로 각기 다른 모양으로 제작된다고 합니다. 전국에 택시 갓등의 종류만 200개가 넘는다고 알려져 있습니다.
"""

okt = Okt()
words = okt.nouns(article)
word_list = [(word,) for word in words]

sc = SparkContext(appName="KoNLPyExample")
spark = SparkSession(sc)
df = spark.createDataFrame(word_list, ["word"])

word_counts = df.rdd.map(lambda word: (word['word'], 1)).reduceByKey(lambda a, b: a + b).collect()

for word, count in word_counts:
    word_str = word
    table.put(
        word_str.encode("utf-8"), 
        {'cf:count': str(count)}
    )
