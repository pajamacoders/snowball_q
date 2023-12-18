def decompose_report_to_fs_is_cf(corp_code:str, year:str, quater:str, fs)->dict:
    res = {"stock_code":corp_code,
        "year":int(year),
        "quarter":quater}
    qfs = get_fs_info_by_quarter(fs)
    qis = get_is_info_by_quater(fs)
    qcf = get_cf_info_by_quarter(fs)
    res.update(qfs)
    res.update(qis)
    res.update(qcf)
    return res

def get_account_value_by_key(table, keys):
    val = None
    for k in keys:
        if k in table.account_nm.to_list():
            val = int(float(table[table.account_nm==k].thstrm_amount.to_list()[0].replace(',','')))
            break
    if val is None:
        print('debug')
    return val

def get_fs_info_by_quarter(fs):
    # 표준계정 코드 account_id 이용해 검색하는 것으로 재구현 할것.
    qfs = fs[fs.sj_div=="BS"]
    
    cur_asset_keys = ["유동자산", 'Ⅰ. 유동자산']
    cur_assets = get_account_value_by_key(qfs, cur_asset_keys)
    noncur_asset_keys = ["비유동자산", 'Ⅱ. 비유동자산']
    non_cur_assets = get_account_value_by_key(qfs, noncur_asset_keys)
    full_assets_keys = ["자산총계"]
    full_assets = get_account_value_by_key(qfs, full_assets_keys)
    cur_liability_keys = ["유동부채", 'Ⅰ. 유동부채']
    cur_liabilities = get_account_value_by_key(qfs, cur_liability_keys)
    noncur_liability_keys = ["비유동부채", 'Ⅱ. 비유동부채']
    noncur_liabilities = get_account_value_by_key(qfs, noncur_liability_keys)
    full_liability_keys = ["부채총계"]
    full_liabilities = get_account_value_by_key(qfs, full_liability_keys)
    cap_keys = ["자본금"]
    issued_capital = get_account_value_by_key(qfs, cap_keys)
    retain_earning_keys = ["이익잉여금(결손금)", "이익잉여금"]
    retain_ernings = get_account_value_by_key(qfs, retain_earning_keys)
    if retain_ernings is None:
        retain_ernings=0
    full_equity_keys = ["자본총계", "지배기업의 소유주자본"]
    full_equity = get_account_value_by_key(qfs, full_equity_keys)
        
    return {
        "current_assets":cur_assets,
        "noncurrent_asset":non_cur_assets,
        "full_assets":full_assets,
        "current_liabilities":cur_liabilities,
        "noncurrent_liabilities":noncur_liabilities,
        "full_liabilities":full_liabilities,
        "issued_capital":issued_capital,
        "retain_ernings":retain_ernings,
        "full_equity":full_equity
    }
  
# def get_account_value_by_key(is_info, keys):
#     val = None
#     for k in keys:
#         if k in is_info.account_nm.to_list():
#             val = int(float(is_info[is_info.account_nm==k].thstrm_amount.to_list()[0].replace(',','')))
            
#     if val is None:
#         print('none')
#     return val
  
def get_is_info_by_quater(fs):
    # 표준계정 코드 account_id 이용해 검색하는 것으로 재구현 할것.
    type_keys = ['손익계산서','포괄손익계산서']
    for k in type_keys:
        if k in fs.sj_nm.to_list():    
            is_info=fs[fs.sj_nm==k]
            break

    revenue_keys = ["수익(매출액)", "매출", "매출액"]
    revenue = get_account_value_by_key(is_info, revenue_keys)
    gross_profit_keys = ["매출총이익",'매출총이익(손실)']
    gross_profit = get_account_value_by_key(is_info, gross_profit_keys)
    operating_income_keys = ["영업이익(손실)", "영업이익", "영업손실",'영업손실(이익)']
    operating_income = get_account_value_by_key(is_info, operating_income_keys)
    fin_cost_key = ["금융비용", "금융원가", "순금융수익(원가)", '순금융원가(수익)','금융수익']
    fin_cost = get_account_value_by_key(is_info, fin_cost_key)
    income_bf_tax_keys = ["법인세비용차감전순이익(손실)","법인세비용차감전순이익", "법인세비용차감전순손실"]
    income_bf_tax = get_account_value_by_key(is_info, income_bf_tax_keys)
    if income_bf_tax is None:
        income_bf_tax=float(is_info[is_info.account_nm=="당기순이익(손실)"].thstrm_amount.to_list()[0])+float(is_info[is_info.account_nm=="법인세비용"].thstrm_amount.to_list()[0])
    
    net_profit_keys = ["당기순이익(손실)","당기순이익", '당기순손실',"분기순이익(손실)", \
        '분기순손실', "반기순손실", "반기순이익(손실)"]
    net_profit = get_account_value_by_key(is_info, net_profit_keys)
    eps_keys =["희석주당이익(손실)", "희석주당순이익(손실)","희석주당이익",'희석주당순손실',\
        "계속영업기본주당이익(손실)", "계속영업기본및희석주당순손익",\
        '계속사업 희석주당순이익(손실)','계속영업희석주당이익(손실)',\
        '기본주당순손실', '기본주당이익(손실)', '희석주당반기순손실','희석반기주당이익(손실)']
    eps = get_account_value_by_key(is_info, eps_keys)
    return {
        "revenue":revenue,
        "gross_progit":gross_profit,
        "operating_income":operating_income,
        "finance_cost": fin_cost,
        "income_befor_tax":income_bf_tax,
        "net_profit":net_profit,
        "diluted_eps":eps
        }
    
def get_cf_info_by_quarter(fs):
    # 표준계정 코드 account_id 이용해 검색하는 것으로 재구현 할것.
    qcf = fs[fs.sj_nm=="현금흐름표"]
    op_cash_flow_keys = ["영업활동현금흐름", "영업활동으로 인한 현금흐름"]
    op_cash_flow = get_account_value_by_key(qcf, op_cash_flow_keys)
    invest_cash_flow_keys = ["투자활동현금흐름", "투자활동으로 인한 현금흐름"]
    invest_cash_flow = get_account_value_by_key(qcf, invest_cash_flow_keys)
    fin_act_cash_flow_keys = ["재무활동현금흐름", "재무활동으로 인한 현금흐름"]
    fin_act_cash_flow = get_account_value_by_key(qcf, fin_act_cash_flow_keys)
    return {
        "cfo":op_cash_flow,
        "cfi":invest_cash_flow,
        "cff":fin_act_cash_flow
    }
    
