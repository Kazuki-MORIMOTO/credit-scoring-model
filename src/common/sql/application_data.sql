select
  t1.contract_id as "契約ID",
  t1.member_id as "メンバーID",
  t1.contract_name as "区分",
  t1.contract_plan_name as "契約種別",
  t1.contract_plan_detail_name as "契約プラン詳細名",
  t1.contract_status_name as "契約ステータス",
  t1.credit_entry_datetime as "与信審査申込日時",
  t1.contract_entry_datetime as "本契約申込日時(契約日)",
  t1.cancellation_status_sv as "中途解約ステータスSV",
  t1.cancellation_status_name as "中途解約",
  t1.sales_channel as "商流",
  t1.vendor_block as "販売会社地区",
  t1.vendor_name as "販売店名（正式名称）",
  t1.store_name as "店舗名称",
  t1.manufacturer_name as "メーカー名",
  t1.car_model_name_display as "車種名（表示用）",
  t1.car_model_category as "車種カテゴリ",
  t1.grade_name as "グレード名",
  t1.outer_plate_color_display_name as "外板色表示名",
  t1.package_display_name as "パッケージ表示名",
  t1.contract_plan_detail_one_name as "契約プラン名",
  t1.owned_manufacturer_name as "現保有車",
  t1.new_used_car_name as "現保有車区分",
  t1.contractors_prefecture_name as "都道府県",
  t1.city as "アクセス元市町村",
  t1.age as "年齢",
  t1.gender_name as "性別",
  t1.office_type as "職業",
  t1.annual_income as "年収",
  t1.application_fee as "申込金税別額",
  t1.vendor_prefecture_name as "販売店都道府県",
  t1.monthly_fee as "月額利用料",
  t1.bonus_addition_january as "ボーナス加算1月",
  t1.bonus_addition_july as "ボーナス加算7月",
  t1.using_purpose_name as "主な使用目的名",
  t1.referral_code as "紹介コード",
  t1.credit_result_name as "承認結果",
  t1.contract_plan_sv as "契約プランSV",
  t1.contract_plan_detail_sv as "契約プラン詳細SV",
  (
        case 
        when t1.contract_plan_detail_sv = 1 and t1.contract_plan_detail_one_name = '3年' then 'TOYOTA 初期フリ3年'
        when t1.contract_plan_detail_sv = 1 and t1.contract_plan_detail_one_name = '5年' then 'TOYOTA 初期フリ5年'
        when t1.contract_plan_detail_sv = 1 and t1.contract_plan_detail_one_name = '7年' then 'TOYOTA 初期フリ7年'
        when t1.contract_plan_detail_sv = 2 and t1.contract_plan_detail_one_name = '3年' then 'LEXUS 3年'
        when t1.contract_plan_detail_sv = 3 and t1.contract_plan_detail_one_name = '3年' then 'FLEX 6 (3年6台)'
        when t1.contract_plan_detail_sv = 4 and t1.contract_plan_detail_one_name = '3年' then 'FLEX 6 (3年3台)'
        when t1.contract_plan_detail_sv = 5 and t1.contract_plan_detail_one_name = '3年'  then 'TOYOTA 解フリ3年'
        when t1.contract_plan_detail_sv = 6 and t1.contract_plan_detail_one_name = '3年'  then 'LEXUS 解フリ3年'
        when t1.contract_plan_detail_sv = 7 then '中古車'
        when t1.contract_plan_detail_sv = 8 then 'bz4x'
        when t1.contract_plan_detail_sv = 9 then '再契約'
        end
    ) as "契約プラン詳細"
from
  prod_kintojpn_view.view_sm_one_newcar_contracts as t1 /* 契約・申し込み情報マート */

