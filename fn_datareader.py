# 1996-12-11~
import FinanceDataReader as fdr # 현재 시장 상장사 및 상장사 주식 가격 정보
import dart_fss as dfss
import OpenDartReader # 재무 데이터 
import requests
from bs4 import BeautifulSoup
from cus_stock import CStock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from financial_statement import Company, FStatements, Base
from extract_financial_factor import *
import pickle
from keys import *
dfss.set_api_key(api_key=api_key)
dart = OpenDartReader(api_key) 

# SQLite 데이터베이스에 연결하는 엔진 생성
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

# 데이터베이스와 테이블 생성
# Base.metadata.create_all(engine)

# 세션 생성
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# session = SessionLocal()

def check_suffix(value, suffix):
    return value.endswith(suffix)

def get_corp_list():
    stock_list = fdr.StockListing("KRX")
    # remove konex
    stock_list = stock_list.loc[stock_list["Market"] != "KONEX"]
    # remove stock without region because this kind of stock is not a normal company stock
    # stock_list =  stock_list.loc[stock_list["Region"].notnull()]
    stock_list = filter_non_corporation(stock_list)
    return stock_list

def filter_non_corporation(stock_list):
    """dataframe contains stocklist
    """
    target_suffix = '0'
    prior_share_mask = stock_list.Code.apply(lambda x: check_suffix(x, target_suffix))
    stock_list = stock_list[prior_share_mask]
    #리츠주 제거, 리츠 주는 이름에 대부분 "n호" 라는 패턴이 들어가므로 이를 이요해 제거
    reit_pattern = r'\d+호' 
    stock_list = stock_list[~stock_list["Name"].str.contains(reit_pattern)]
    # 스팩 주 제거
    spac_pattern = r'스팩' 
    stock_list = stock_list[~stock_list["Name"].str.contains(spac_pattern)]
    return stock_list

def get_copr_list_of_bottm_20percent_marcap(corp_list, btm_ratio:float=0.2):
    """
    상장 기업 중 시가총액 기준 하위 btm_ratio 에 해당하는 기업 리스트 리턴
    """
    btm_25per_idx = int(len(corp_list)*btm_ratio)
    small_cap = corp_list[-btm_25per_idx:]
    small_corp_info = []
    for symbol, name, marcap, n_shares, market in small_cap:
        
       small_corp_info.append(CStock(symbol, name, marcap, n_shares, market, dart))
    small_corp_info = [corp for corp in small_corp_info if corp.is_report_availiable()]
    return small_corp_info

def fetch_listing_date(company_code):
    api_url = 'https://api.openapipartners.com/public_info/v1/company_listing_date/{}'.format(company_code)
    headers = {
        'Authorization': 'Bearer {}'.format(api_key),
    }

    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        listing_date = response_data['listing_date']  # listing_date에 실제 필드명을 사용
        return listing_date
    else:
        return None
# reprt_code  보고서 코드  STRING(5) Y 1분기보고서 : 11013
# 반기보고서 : 11012
# 3분기보고서 : 11014
# 사업보고서 : 11011

    
    
if __name__=="__main__":
    #기업정보
    if 1:
        crp_list=dfss.get_corp_list()
        with open(file='crp_list.pickle', mode='wb') as f:
            pickle.dump(crp_list, f)
    else:
        with open(file='crp_list.pickle', mode='rb') as f:
            crp_list = pickle.load(f)

    st_list = get_corp_list()
    for idx, row in st_list.iterrows():
        corp = crp_list.find_by_stock_code(row.Code)
        # listing_date = fetch_listing_date(corp.corp_code)
        #dart.company('005930')
        {'corp_code':corp.corp_code,
        'stock_code':row.Code,
         'corp_name':row.Name,
         'market': row.Market,
         'sector': corp.sector
         }
        fs_quarter = corp.extract_fs(bgn_de='20110101',end_de='20131231', report_tp='quarter')
        fs_list = get_financial_statements_factor_vb2015(row.Code, fs_quarter)
        dbs = fs_quarter['bs']
        # dbs_columns = dbs.columns.to_flat_index()[7:]
        #bs_labels = fs_quarter.labels['bs']
        # dis=fs_quarter['is']
        # is_labels = dis.columns.to_flat_index()
        is_list = get_income_statements_factor(row.Code, fs_quarter)
        dcf=fs_quarter['bs']
        cf_labels = dcf.columns.to_flat_index()
        print(corp)
    print(st_list)
