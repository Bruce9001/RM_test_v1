import mysql.connector
import pandas
import datetime
from datetime import timedelta
import psycopg2
import numpy
from dateutil.relativedelta import relativedelta
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.ensemble import IsolationForest
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def get_start_end_dates(year, month):
    # 월의 시작 날짜: 항상 1일
    start_date = datetime.date(year, month, 1)
    # 다음 달의 첫 날짜 계산
    if month == 12:
        # 12월인 경우, 다음 해의 1월 1일을 계산
        next_month = datetime.date(year + 1, 1, 1)
    else:
        # 그 외의 경우, 다음 달의 1일을 계산
        next_month = datetime.date(year, month + 1, 1)
    # 월의 마지막 날짜: 다음 달의 첫 날에서 하루를 빼면 됨
    end_date = next_month + relativedelta(days=-1)
    return start_date, end_date

# 해당 주차 요일 다 가져오는 함수
def get_week_dates(date_str):
    # 문자열로부터 datetime 객체 생성
    given_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    
    # 주의 첫 날(월요일) 구하기
    start_date = given_date - timedelta(days=given_date.weekday())
    
    # 주의 모든 날짜를 저장할 리스트
    week_dates = []
    
    # 월요일부터 일주일 동안의 날짜를 리스트에 추가
    for i in range(7):
        week_day = start_date + timedelta(days=i)
        week_dates.append(week_day.strftime('%Y-%m-%d'))
    
    return week_dates

# 계절성 분석 함수
def analyze_seasonality(data, column, period=7):
    if len(data) >= period * 2:  # 최소 2주기의 데이터가 필요
        result = seasonal_decompose(data[column], model='additive', period=period)
        return result.seasonal
    else:
        return pandas.Series(0, index=data.index)  # 충분한 데이터가 없으면 0으로 채운 시리즈 반환

# 이상치 탐지 함수
def detect_anomalies(data, columns, contamination=0.07):
    if len(data) > 1:  # 데이터가 충분한 경우에만 이상치 탐지 수행
        clf = IsolationForest(contamination=contamination, random_state=42)
        anomalies = clf.fit_predict(data[columns])
        return anomalies == -1
    else:
        return numpy.array([])  # 데이터가 부족하면 빈 배열 반환

# 계절성 제거 함수
def remove_seasonality(data, seasonality, columns_for_analysis):
    for col in columns_for_analysis:
        if len(seasonality[col]) > 0:
            data[f'deseasonalized_{col}'] = data[col] - seasonality[col][data.index.dayofweek].values
        else:
            data[f'deseasonalized_{col}'] = data[col]
    return data

# 이상치 방향 확인 함수
def check_outlier_direction(data, outlier_index, window=4):
    moving_avg = data.rolling(window=window).mean()
    return "상승" if data.iloc[outlier_index] > moving_avg.iloc[outlier_index] else "하락"


# 데이터 추출, 레드시프트 데이터 당월, Mysql 전일, MS, 경쟁사대수, 날씨, 
class DATA_EXT:
    
    # 초기 설정
    def __init__(self):
        self.Today_date = datetime.date.today() + relativedelta(days=-1)
        self.S_date = datetime.date.today() + relativedelta(months=-1)

        self.E_date = self.Today_date
        self.region_name = ['경북RS팀','부경RS팀']
        self.depth = 1

        self.DATA = None

        self.region_mapping = self.region_mapping()

        print('Connection ok, Initialization is done')

    # 첫 호출로 지역, 날짜 확인
    def __call__(self,region_name=['경북RS팀'], depth=1):
        
        self.region_name = region_name
        self.depth = depth

        if depth == 1:
            self.region_mapping = self.region_mapping.loc[self.region_mapping['high'].isin(region_name)]
        else:
            self.region_mapping = self.region_mapping.loc[self.region_mapping['middle'].isin(region_name)]   
        
        print('\n', self.S_date, ' -> ' , self.E_date)

        self.get_sales_data()
        self.DATA['date'] = pandas.to_datetime(self.DATA['date'])

        self.weather_data = self.get_weather_data()
        self.newuser_data = self.get_newuser_data()

        return self.DATA
    
    # 날짜 타입에 맞춰서 시작, 종료일 계산
    def date_cal(self, date, d_type = 'M'):

        CM_start = None
        CM_end = None

        if d_type == 'M':
            CM_start, CM_end = get_start_end_dates(date.year, date.month)

        elif d_type == 'W':
            W_list = get_week_dates(str(date))
            CM_start = datetime.datetime.strptime(W_list[0], '%Y-%m-%d')
            CM_end = datetime.datetime.strptime(W_list[6], '%Y-%m-%d') + relativedelta(days=1)
        
        elif d_type == 'D':
            CM_start = date
            CM_end = date + relativedelta(days=1)
        
        self.S_date = CM_start
        self.E_date = CM_end
        print(self.S_date, ' -> ' , self.E_date)

    # SQL 데이터 추출 함수
    def get_sql_data(self,query,sw=1):
        # (2) Redshift_daily 매출 기록 연결
        if sw == 1:
            cnx_redshift = psycopg2.connect(database="gbike",
                                    host="live-redshift-cluster-gbike.comng6zxlpsm.ap-northeast-2.redshift.amazonaws.com",
                                    user="rmteam",
                                    password="Gbike@rmteam@20230510!",
                                    port="5439")
            data = pandas.read_sql(query,cnx_redshift)
            # (2)연결 종료
            cnx_redshift.close()
        elif sw == 0:
            
            cnx = mysql.connector.connect(user='gbikemarketing', password='gbikemkt0514$@#!', 
                                host='live.st.rds.gbility.io')
            data = pandas.read_sql(query,cnx)
            cnx.close()

        # 대학 대항전 메일주소 복호화 코드 설정을 위해 SET 함수 먼저 사용 필요!
        elif sw == 2:
            cnx = mysql.connector.connect(user='gbikemarketing', password='gbikemkt0514$@#!', 
                                host='live.st.rds.gbility.io', database='gbike')
            
            if cnx.is_connected():
                cursor = cnx.cursor()
                # 현재 선택된 데이터베이스 확인
                cursor.execute("SELECT DATABASE()")
                db_name = cursor.fetchone()[0]
                print(f"Connected to database: {db_name}")
                if db_name is None:
                    cursor.execute("USE your_database")  # 'your_database'를 실제 데이터베이스 이름으로 변경하세요
                    print(f"Switched to database: your_database")

                cursor.execute("SET @passphrase = MD5('VisitMildStingExcess7');")
                cursor.execute("SET @@session.block_encryption_mode = 'aes-256-cbc';")
                data = pandas.read_sql(query,cnx)
                cursor.close()
                cnx.close()
        return data
    
    # 지역정보 확인
    def region_mapping(self):
    
        query = """select A.region_id, A.region_name as low, B.region_name as middle, C.region_name as high from gbike.rich_region as A
                    left join gbike.rich_region as B on A.parent_id = B.region_id
                    left join gbike.rich_region as C on B.parent_id = C.region_id
                    where (A.is_used = 'Y') """

        data = self.get_sql_data(query,0)

        return data
    
    # 데이터 추출
    def get_sales_data(self):

        red_sales_query = """SELECT O_CM.region_id, O_CM.date,
            O_CM.low_region_name, O_CM.assigned_count, O_CM.deployed_count, O_CM.order_count, O_CM.calculated_pay_amount,
            O_CM.calculated_out_of_area_charge,
            CASE
                WHEN O_CM.model IN ('Max Pro','Max Plus','Max','Max Plus X','MAX PLUS','MAX PRO','MAX','PLUS-X','GCOO-K2','K2') THEN 'Scooter'
                WHEN O_CM.model IN ('GCOO-B2','Glide-B2','GCOO-B3') THEN 'Bicycle'
                WHEN O_CM.model IN ('GCOO-K1','ES4','세그웨이 자전거','지쿠터 K') THEN 'Etc'
                ELSE O_CM.model
            END AS Model,
            CASE
                WHEN O_CM.middle_region_name = '부산캠프' THEN '부산1캠프'
                WHEN O_CM.middle_region_name = '제천캠프' THEN '충주캠프'
                WHEN O_CM.middle_region_name = '대전캠프' THEN '대전1캠프'
                WHEN O_CM.middle_region_name = '대구캠프' THEN '대구1캠프'
                WHEN O_CM.middle_region_name = '고양캠프' THEN '고양1캠프'
                WHEN O_CM.middle_region_name = '강남캠프' THEN '송파캠프'
                WHEN O_CM.high_region_name = '가맹영업팀' THEN '가맹영업팀'
                ELSE O_CM.middle_region_name
            END AS middle_region_name,
            CASE
                WHEN O_CM.middle_region_name IN ('대구1캠프','대구2캠프','구미캠프','포항캠프','대구캠프') THEN '경북RS팀'
                WHEN O_CM.middle_region_name IN ('부산캠프','부산1캠프','부산2캠프') THEN '부경RS팀'
                WHEN O_CM.middle_region_name IN ('광주1캠프','광주2캠프','목포캠프','여수캠프','제주캠프','서귀포캠프') THEN '남부RS팀'
                WHEN O_CM.middle_region_name IN ('고양캠프','고양1캠프','고양2캠프','의정부캠프','노원캠프','원주캠프','강릉캠프') THEN '서울RS팀'
                WHEN O_CM.middle_region_name IN ('강남캠프','서초캠프','송파캠프','성남캠프','용인캠프','동탄캠프') THEN '강남RS팀'
                WHEN O_CM.middle_region_name IN ('송도캠프','청라캠프','안산캠프','김포캠프','광명캠프') THEN '경인RS팀'
                WHEN O_CM.middle_region_name IN ('세종캠프','청주캠프','대전캠프','대전1캠프','대전2캠프','천안캠프','충주캠프','평택캠프','제천캠프') THEN '중부RS팀'
                ELSE O_CM.high_region_name
            END AS high_region_name
        FROM gbike.rich_daily_statistics as O_CM
        WHERE (O_CM.high_region_name LIKE '%팀') AND (O_CM.middle_region_name NOT LIKE '폐기%') and (O_CM.date between '{}' and '{}')""".format(str(self.S_date),str(self.E_date))

        self.DATA = self.get_sql_data(red_sales_query,1)

        if self.depth == 1:
            self.DATA = self.DATA.loc[self.DATA['high_region_name'].isin(self.region_name)]
        else:
            self.DATA = self.DATA.loc[self.DATA['middle_region_name'].isin(self.region_name)]
        self.DATA = self.DATA.groupby(['date','region_id','high_region_name','middle_region_name','low_region_name','model']).sum().reset_index()
        self.DATA['Rev'] = (self.DATA['calculated_pay_amount'] + self.DATA['calculated_out_of_area_charge'])/1.1

    # 날씨 데이터 추출  
    def get_weather_data(self):

        region_name_str = ','.join(f"'{region_name}'" for region_name in self.region_name)
        
        if self.depth == 1:
            query = """ SELECT DATE(w.date) as date, w.region_id, r1.region_name as low_region_name, r2.region_name as middle_region_name, r3.region_name as high_region_name,
                        avg(w.temperature) as '평균기온', sum(w.precipitation) as '강수량' FROM gbike_smartops.weather_data as w
                        left join gbike.rich_region as r1 on r1.region_id = w.region_id
                        left join gbike.rich_region as r2 on r1.parent_id = r2.region_id
                        left join gbike.rich_region as r3 on r2.parent_id = r3.region_id
                        where (w.date between '{}' and '{}') and (r3.region_name in ({})) and (w.temperature != -999) and (w.precipitation != -999)
                        group by date, w.region_id, r3.region_name, r2.region_name, r1.region_name""".format(str(self.S_date),str(self.E_date),region_name_str)
        else:
            query = """ SELECT DATE(w.date) as date, w.region_id, r1.region_name as low_region_name, r2.region_name as middle_region_name, r3.region_name as high_region_name,
                        avg(w.temperature) as '평균기온', sum(w.precipitation) as '강수량' FROM gbike_smartops.weather_data as w
                        left join gbike.rich_region as r1 on r1.region_id = w.region_id
                        left join gbike.rich_region as r2 on r1.parent_id = r2.region_id
                        left join gbike.rich_region as r3 on r2.parent_id = r3.region_id
                        where (w.date between '{}' and '{}') and (r2.region_name in ({})) and (w.temperature != -999) and (w.precipitation != -999)
                        group by date, region_id, r3.region_name, r2.region_name, r1.region_name""".format(str(self.S_date),str(self.E_date),region_name_str)
        
        data = self.get_sql_data(query,0)

        # data = data.loc[~(data['평균기온'] <= -20)]
        data.loc[data['강수량'] < 0, '강수량'] = 0
        data.loc[data['평균기온'] < -20,'평균기온'] = data['평균기온'].mean()
    
        return data

    # 신규가입자 데이터 추출
    def get_newuser_data(self):
        region_name_str = ','.join(f"'{region_name}'" for region_name in self.region_name)

        if self.depth == 1:
            query = """SELECT FROM_UNIXTIME(u.add_time + 32400, '%Y-%m-%d') as date, 
                        u.register_region_id as region_id, r1.region_name as low_region_name, r2.region_name as middle_region_name, r3.region_name as high_region_name, count(u.user_id) as new_user FROM gbike.rich_user as u
                        left join gbike.rich_region as r1 on u.register_region_id = r1.region_id
                        left join gbike.rich_region as r2 on r1.parent_id = r2.region_id
                        left join gbike.rich_region as r3 on r2.parent_id = r3.region_id
                        where (FROM_UNIXTIME(u.add_time + 32400) between '{}' and '{}')
                        and (r3.region_name in ({}))
                        group by FROM_UNIXTIME(u.add_time + 32400, '%Y-%m-%d'), u.register_region_id, r3.region_name, r2.region_name, r1.region_name""".format(str(self.S_date),str(self.E_date + relativedelta(days=1)),region_name_str)
        else:
            query = """SELECT FROM_UNIXTIME(u.add_time + 32400, '%Y-%m-%d') as date, 
                        u.register_region_id as region_id, r1.region_name as low_region_name, r2.region_name as middle_region_name, r3.region_name as high_region_name, count(u.user_id) as new_user FROM gbike.rich_user as u
                        left join gbike.rich_region as r1 on u.register_region_id = r1.region_id
                        left join gbike.rich_region as r2 on r1.parent_id = r2.region_id
                        left join gbike.rich_region as r3 on r2.parent_id = r3.region_id
                        where (FROM_UNIXTIME(u.add_time + 32400) between '{}' and '{}')
                        and (r2.region_name in ({}))
                        group by FROM_UNIXTIME(u.add_time + 32400, '%Y-%m-%d'), u.register_region_id, r3.region_name, r2.region_name, r1.region_name""".format(str(self.S_date),str(self.E_date + relativedelta(days=1)),region_name_str)

        data = self.get_sql_data(query,0)

        return data
    
    # Mysql 매출 확인 (최근 한달 주문 출력)
    def get_yesterday_sales_fromMYSQL(self):
        region_name_str = ','.join(f"'{region_name}'" for region_name in self.region_name)
        
        if self.depth == 1:
            query = """SELECT FROM_UNIXTIME(o.add_time + 32400, '%Y-%m-%d') as date, o.add_time,
                        CASE
                            WHEN o.bicycle_sn < 600000 THEN 'Scooter'
                            WHEN o.bicycle_sn >= 600000 THEN 'Bicycle'
                            ELSE 'Etc'
                        END AS model,
                        r1.region_name as low_region_name, r2.region_name as middle_region_name, r3.region_name as high_region_name,
                        o.region_id, o.user_id, o.bicycle_sn, o.order_id, o.start_lat, o.start_lng, o.end_lat, o.end_lng, o.pay_amount, o.out_of_area_charge
                        FROM gbike.rich_orders as o
                        left join gbike.rich_region as r1 on o.region_id = r1.region_id
                        left join gbike.rich_region as r2 on r1.parent_id = r2.region_id
                        left join gbike.rich_region as r3 on r2.parent_id = r3.region_id
                        WHERE (FROM_UNIXTIME(o.add_time + 32400, '%Y-%m-%d') between '{}' and '{}') and (o.order_state = 2) and (r3.region_name in ({}))""".format(str(self.S_date),str(self.E_date + relativedelta(days=1)), region_name_str)
        else:
            query = """SELECT FROM_UNIXTIME(o.add_time + 32400, '%Y-%m-%d') as date, o.add_time,
                        CASE
                            WHEN o.bicycle_sn < 600000 THEN 'Scooter'
                            WHEN o.bicycle_sn >= 600000 THEN 'Bicycle'
                            ELSE 'Etc'
                        END AS model,
                        r1.region_name as low_region_name, r2.region_name as middle_region_name, r3.region_name as high_region_name,
                        o.order_id, o.user_id, o.bicycle_sn, o.start_lat, o.start_lng, o.end_lat, o.end_lng, o.pay_amount, o.out_of_area_charge 
                        FROM gbike.rich_orders as o
                        left join gbike.rich_region as r1 on o.region_id = r1.region_id
                        left join gbike.rich_region as r2 on r1.parent_id = r2.region_id
                        left join gbike.rich_region as r3 on r2.parent_id = r3.region_id
                        WHERE (FROM_UNIXTIME(o.add_time + 32400, '%Y-%m-%d') between '{}' and '{}') and (o.order_state = 2) and (r2.region_name in ({}))""".format(str(self.S_date),str(self.E_date + relativedelta(days=1)), region_name_str)
        
        data = self.get_sql_data(query,0)
        data['Rev'] = (data['pay_amount'] + data['out_of_area_charge'])/1.1

        if self.DATA['date'].dt.strftime('%Y-%m-%d').max() != str(self.Today_date):
            print(self.DATA['date'].dt.strftime('%Y-%m-%d').max(), self.Today_date)
            print('Redshift 매출 데이터 없음')
            print('Mysql 데이터 확인')
            print(data.loc[data['date'] == str(self.Today_date)].groupby(['date','middle_region_name'])['Rev'].sum().reset_index())

            self.DATA = pandas.concat([self.DATA,data.loc[data['date'] == str(self.Today_date)][['date','region_id','model', 'low_region_name', 'middle_region_name','high_region_name','Rev']]  ],axis=0)

        else:
            print('Mysql 데이터 확인')
            print(data.loc[data['date'] == str(self.Today_date)].groupby(['date','middle_region_name'])['Rev'].sum().reset_index())
            print('Redshift 데이터 확인')
            print(self.DATA.loc[self.DATA['date'] == str(self.Today_date)].groupby(['date','middle_region_name'])['Rev'].sum().reset_index())

        return data


# 데이터 정제, 시각화
class DATA_EDA_SHOW:
    
    # 초기 설정
    def __init__(self, data):
        
        print('red_Data 확인, 정제 진행')
        self.DATA = data
        self.color = ["#9BBB59","#7030A0","#FF0000","#FFC000","#4F81BD","#B9CDE5","#000080","#4F6228","#C00000","#FF9B00","#32BEBE"]
        self.region_info = 'middle_region_name'
        self.region_label = '캠프명'
        self.data_info = 'Rev'
        self.S_date = data['date'].min()
        self.E_date = data['date'].max()
        self.anomalies = None

        try:
            print('데이터 기간 :',self.S_date,' ->', self.E_date)
            self.DATA['date'] = pandas.to_datetime(self.DATA['date'])
            self.DATA['Year'] = self.DATA['date'].dt.year
            self.DATA['Month'] = self.DATA['date'].dt.month
            self.DATA['Week_num'] = self.DATA['date'].dt.isocalendar().week
            self.DATA['Weekday'] = self.DATA['date'].dt.day_name()
        except KeyError:
            print('날짜 정보 없음')
        
        try:
            print('데이터 종류 :',self.DATA['high_region_name'].unique())
        except KeyError:
            print('지역 정보 없음')
        
        try:
            self.DATA = self.DATA.fillna(0)
            self.DATA = self.DATA.replace([numpy.inf, -numpy.inf], 0)
        except KeyError:
            print('매출 정보 없음')

    # 데이터 필터
    def data_filter(self, model_filter = ['Scooter','Bicylce'], camp_filter = None, depth = 1):
        
        # 필터 적용
        self.DATA = self.DATA.loc[self.DATA['model'].isin(model_filter)]
        if camp_filter is not None:
            self.DATA = self.DATA.loc[self.DATA['middle_region_name'].isin(camp_filter)]

    # 최근 2주 데이터 시각화
    def vis_s1(self, data = None, data_info = 'Rev', depth = 1):
        
        self.data_info = data_info

        if depth == 1:
            self.region_info = 'middle_region_name'
            self.region_label = '캠프명'
        elif depth == 2:
            self.region_info = 'low_region_name'
            self.region_label = '소지역명'

        if data is None:
            table = self.DATA.loc[self.DATA['date'] >= str(self.E_date + relativedelta(days=-14))]

            table = table.groupby([self.region_info,'date'])[['Rev','assigned_count', 'order_count', 'deployed_count']].sum().reset_index()
            table = self.table_cal(table)
        elif data_info == 'new_user':
            data['date'] = pandas.to_datetime(data['date'])
            data = data.loc[data['date'] >= str(self.E_date + relativedelta(days=-14))]
            table = data.groupby([self.region_info,'date'])[self.data_info].sum().reset_index()
        elif data_info in ['평균기온','강수량']:
            data['date'] = pandas.to_datetime(data['date'])
            data = data.loc[data['date'] >= str(self.E_date + relativedelta(days=-14))]
            table = data.groupby([self.region_info,'date'])[self.data_info].mean().reset_index()

        if depth == 1:
            if self.data_info in ['Rev','new_user']:
                fig = px.bar(table, x="date", y=self.data_info, color=self.region_info, text_auto='.2s', color_discrete_sequence=self.color)
            else:
                fig = px.line(table, x="date", y=self.data_info, color=self.region_info, width= 700, markers=True, color_discrete_sequence=self.color)
                
                for i in table[self.region_info].unique():
                    fig.add_annotation(x = self.E_date, 
                                       y = table.loc[(table['date'] == str(self.E_date)) & (table[self.region_info] == i)][self.data_info].values[0],
                                       text="{:,.0f}".format(table.loc[(table['date'] == str(self.E_date)) & (table[self.region_info] == i)][self.data_info].values[0]),
                                       showarrow=True)
        
        elif depth == 2:
            if self.data_info in ['Rev','new_user']:
                fig = px.bar(table, x="date", y=self.data_info, color=self.region_info, text_auto='.2s',)
            else:
                fig = px.line(table, x="date", y=self.data_info, color=self.region_info, width= 700, markers=True)

        label_dict = { "Rev": "매출", "Rev per unit": "대당매출", "assigned_count": "할당대수", "order_count": "운행수","deployed_count" : "배치대수", 
                      "OC per unit" : "대당회전수", "OP rate": "가동률", "new_user": "가입자수", "평균기온": "평균기온", "강수량": "강수량"}
        
        fig.update_layout(
            title=str(self.E_date.strftime('%Y-%m-%d')) + " " + self.region_label[:-1] + "별 " + label_dict[self.data_info],
            xaxis_title="날짜",
            yaxis_title=label_dict[self.data_info],
            legend_title=self.region_label,
            )
        fig.show()

    # 매출 변화 보고용
    def vis_exp_1(self):
        
        yesterday = self.E_date + relativedelta(days=-1)
        prev_week = [self.E_date - timedelta(days=x) for x in range(1,8)]

        today_d = self.DATA.loc[self.DATA['date'] == str(self.E_date)].groupby(self.region_info)[self.data_info].sum().reset_index()
        yesterday_d = self.DATA.loc[self.DATA['date'] == str(yesterday)].groupby(self.region_info)[self.data_info].sum().reset_index()
        prev_week_d = self.DATA.loc[self.DATA['date'].isin(prev_week)].groupby(self.region_info)[self.data_info].sum().reset_index()
        prev_4week_d = self.DATA.groupby(self.region_info)[self.data_info].sum().reset_index()


        if self.data_info in ['Rev','assigned_count', 'order_count', 'deployed_count']:
            mean_week_num = len(prev_week)
            mean_month_num = len(self.DATA['date'].unique())-1

        else:
            mean_week_num = 1
            mean_month_num = 1

        print(self.data_info, " 현황")
        print("--------------------------------------")
        for i in self.DATA[self.region_info].sort_values().unique():
            try:
                cal_1 = (today_d.loc[today_d[self.region_info] == i,self.data_info].item()/yesterday_d.loc[yesterday_d[self.region_info] == i,self.data_info].item()) -1
                cal_2 = (today_d.loc[today_d[self.region_info] == i,self.data_info].item()/prev_week_d.loc[prev_week_d[self.region_info] == i,self.data_info].item())*mean_week_num -1
                cal_3 = (today_d.loc[today_d[self.region_info] == i,self.data_info].item()/prev_4week_d.loc[prev_4week_d[self.region_info] == i,self.data_info].item())*mean_month_num -1
                print(i + " 전일 대비: " + '{:.1%}'.format(cal_1))
                print(i + " 지난7일 대비: " + '{:.1%}'.format(cal_2))
                print(i + " 지난30일 대비: " + '{:.1%}'.format(cal_3))
            except ZeroDivisionError:
                print(i," 정보없음")
            except ValueError:
                print(i," 비교 데이터 없음")
            print("--------------------------------------")

    # 대당 관련 지표 계산
    def table_cal(self,table1):

        table1['Rev per unit'] = table1['Rev'] / table1['assigned_count']
        table1['OC per unit'] = table1['order_count'] / table1['assigned_count']
        table1['OP rate'] = table1['deployed_count'] / table1['assigned_count']

        return table1

    # 매출보고용 시퀀스
    def daily_process(self):
        self.vis_s1(data_info='Rev')
        self.vis_exp_1()
        self.vis_s1(data_info='Rev per unit')
        self.anomaly_detect()
        for i in self.anomalies.index:
            self.vis_multi(region_id= self.anomalies.loc[i,'region_id'], model= self.anomalies.loc[i,'model'])

    # 이상치 탐색
    def anomaly_detect(self):
        columns_for_analysis = ['Rev', 'order_count']

        conditions = self.DATA.loc[(self.DATA['date'] == str(self.E_date)) & (self.DATA['assigned_count'] >= 20)][['region_id','model']]

        overall_data = self.DATA.loc[(self.DATA['region_id'].isin(conditions['region_id'].unique()))]
        overall_data = overall_data.groupby('date')[columns_for_analysis].mean().reset_index()
        overall_seasonality = {col: analyze_seasonality(overall_data.set_index('date'), col) for col in columns_for_analysis}
        overall_data = remove_seasonality(overall_data.set_index('date'), overall_seasonality,columns_for_analysis).reset_index()
        
        # 지역별, 기기별 분석
        grouped_data = self.DATA.groupby(['date', 'region_id', 'model'])
        regional_device_data = grouped_data[columns_for_analysis].mean().reset_index()

        # 지역별, 기기별 계절성 분석 및 이상치 탐지
        anomalies_result = []

        for (region, model), group in regional_device_data.groupby(['region_id', 'model']):
            group_data = group.set_index('date')
            if (len(group_data) >= 14):  # 최소 2주 이상의 데이터가 있는 경우, 할당대수가 10대 이상 분석
                seasonality = {col: analyze_seasonality(group_data, col) for col in columns_for_analysis}
                deseasonalized_data = remove_seasonality(group_data, seasonality,columns_for_analysis).reset_index()
                anomalies = detect_anomalies(deseasonalized_data, [f'deseasonalized_{col}' for col in columns_for_analysis])
                
                anomaly_dates = deseasonalized_data.loc[anomalies, 'date']

                for i, is_anomaly in enumerate(anomalies):
                    if is_anomaly:
                        date = deseasonalized_data.iloc[i]['date']
                        direction = check_outlier_direction(group_data['Rev'], i)
                        anomalies_result.append({
                            'date': date, 
                            'region_id': region, 
                            'model': model, 
                            'direction': direction
                        })

        anomalies_df = pandas.DataFrame(anomalies_result)
        anomalies_df = anomalies_df.loc[anomalies_df['date'] == str(self.E_date)][['region_id','model']]
        self.anomalies = anomalies_df

    # 이중 그래프 표현 (대당매출, 할당대수)
    def vis_multi(self, data = None, region_id = None, model = None):
        
        if data is None:
            table = self.DATA.loc[(self.DATA['date'] >= str(self.E_date + relativedelta(days=-14))) & (self.DATA['region_id'] == region_id) & (self.DATA['model'] == model)]
            table = table.groupby(['low_region_name','date'])[['Rev','assigned_count', 'order_count', 'deployed_count']].sum().reset_index()
            table = self.table_cal(table)

            axis_n1 = "할당대수"
            axis_n2 = "대당매출"

            fig = go.Figure()
            fig.add_trace(go.Bar(x=table['date'], y=table['assigned_count'],
                                name=axis_n1, yaxis='y', marker_color='rgb(55, 255, 55)'))
            fig.add_trace(go.Scatter(x=table['date'], y=table['Rev per unit'], 
                                name=axis_n2, yaxis="y2", mode='lines+markers', marker_color='rgb(26, 118, 255)'))
            
            # title
            fig.update_layout(
                title_text= table['low_region_name'].unique()[0] + " " + model + " " + axis_n1 + " 및 " + axis_n2 + " 변화",
            )
            
        else: 
            data['date'] = pandas.to_datetime(data['date'])
            table = data.loc[(data['date'] >= str(self.E_date + relativedelta(days=-14)))]
            region_name = table['high_region_name'].unique()[0]
            table = table.groupby('date')[['평균기온','강수량']].mean().reset_index()

            axis_n1 = "강수량"
            axis_n2 = "평균기온"

            fig = go.Figure()
            fig.add_trace(go.Bar(x=table['date'], y=table[axis_n1],
                                name=axis_n1, yaxis='y'))
            fig.add_trace(go.Scatter(x=table['date'], y=table[axis_n2], 
                                name=axis_n2, yaxis="y2"))
            
            # title
            fig.update_layout(
                title_text= region_name + " " + axis_n1 + " 및 " + axis_n2 + " 변화",
            )

        # Create axis objects
        fig.update_layout(
            #create 1st y axis              
            yaxis=dict(
                title=axis_n1,
                titlefont=dict(color="#1f77b4"),
                tickfont=dict(color="#1f77b4")),             
            #create 2nd y axis       
            yaxis2=dict(title=axis_n2,overlaying="y",
                        side="right"))
        
        
        fig.show()
