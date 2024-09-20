with

-- car_data_dupl_origin 複数レコードに分かれる情報を対象　　　車基本情報、車契約情報、装備情報、単独オプション、内装色
car_data_single_origin as
(
    select
      t1.contract_id as "契約ID",
      t1.contract_admin_number as "契約管理番号(TFC設定項目)",
      t1.contract_status_sv as "契約ステータスSV",
      t1.agreement_datetime as "申込規約同意日時",
      t1.credit_entry_datetime as "与信審査申込日時",
      t1.credit_date as "与信結果確定日",
      t1.contract_entry_datetime as "本契約申込日時(契約日)",
      t1.contract_months as "契約月数",
      t3.car_model_code as "車種コード",
      t3.car_model_name as "車種名",
      t3.car_model_english_name as "車種名（英字）",
      t4.request_plate_number_flag as "希望ナンバーフラグ",
      t4.car_serial_number as "車台番号",
      t4.monthly_plan_travel_distance as "月間予定走行距離",
      t4.support_car_subsidy_sv as "サポカー補助金対応区分SV",
    
      t5.grade_code as "パッケージ_グレードコード",
      t5.package_code as "パッケージコード",
      t5.package_display_name as "パッケージ表示名",
      t5.package_display_name_line_2 as "パッケージ説明文",
      t5.recommended_package_icon_code as "おすすめパッケージアイコンコード",
      t5.monthly_additional_amount_tax_exclude_id as "パッケージ別月額加算額ID",
      t7.price as "パッケージ月額",
    
      t8.grade_code as "グレードコード",
      t8.grade_name as "グレード名",
      t8.boarding_capacity as "乗車定員",
      t8.hv_gas as "HV/GAS",
      t8.car_price_by_model_tax_exclude as "型式別車両本体価格（税抜）",
      t8.certified_model as "認定型式",
      t8.drive as "駆動",
      t8.displacement as "排気量",
      t8.gas_mileage as "燃費",
      t8.transmission as "トランスミッション",
      t8.recommended_grade_icon_code as "おすすめグレードアイコンコード",
      t8.grade_description as "グレード説明文",
      t9.various_price_sv as "グレード_契約月数別変動金額SV",
      t10.price as "グレード_残価"
    
    from
      prod_kintojpn_view.view_rdb_contract as t1 /* 契約 */
      left outer join prod_kintojpn_view.view_rdb_based_car as t2 on /* 車基本情報 */
        t2.contract_id = t1.contract_id
      left outer join prod_kintojpn_view.view_rdb_m_car_model as t3 on /* 車種マスタ */
        t3.car_model_id = t2.car_model_id
      left outer join prod_kintojpn_view.view_rdb_car_contract_limited as t4 on /* 車契約情報 */
        t4.based_car_id = t2.based_car_id
    
      left outer join prod_kintojpn_view.view_rdb_m_package as t5 on /* パッケージマスタ */
        t5.package_id = t2.package_id
      left outer join prod_kintojpn_view.view_rdb_various_price as t6 on /* 契約月数別変動金額 */
        t6.various_price_id = t5.monthly_additional_amount_tax_exclude_id
      left outer join prod_kintojpn_view.view_rdb_various_price_detail as t7 on /* 契約月数別変動金額詳細 */
        t7.various_price_id = t6.various_price_id and t7.contract_month = t1.contract_months
    
      left outer join prod_kintojpn_view.view_rdb_m_grade as t8 on /* グレードマスタ(機密情報含む) */
        t8.grade_id = t2.grade_id
      left outer join prod_kintojpn_view.view_rdb_various_price as t9 on /* 契約月数別変動金額 */
        t9.various_price_id = t8.tfc_guaranteed_residual_value_id
      left outer join prod_kintojpn_view.view_rdb_various_price_detail as t10 on /* 契約月数別変動金額詳細 */
        t10.various_price_id = t9.various_price_id and t10.contract_month = t1.contract_months
    
),

-- car_data_dupl_origin 複数レコードに分かれる情報を対象　　　車基本情報、車契約情報、装備情報、単独オプション、内装色
car_data_dupl_origin as
(
    select
      t1.contract_id as "契約ID",
      t2.car_model_id as "車種ID",
      t3.car_serial_number as "車台番号",
      t4.equipment_id as "装備ID",
      t4.equipment_code as "装備コード",
      t4.equipment_application_sv as "装備申込有無",
      t5.single_option_code as "単独オプションコード",
      t5.single_option_group_code as "単独オプショングループコード",
      t5.single_option_group_display_name as "単独オプショングループ表示名",
      t5.single_option_abbreviation as "単独オプション表示名（略称）",
      t5.single_option_formal_name as "単独オプション表示名（正式名称）",
      t5.single_option_month_fee_id as "単独オプション月額利用料ID",
      t6.various_price_id as "契約月数別変動金額ID",
      t6.various_price_sv as "契約月数別変動金額SV",
      t7.price as "単独オプション月額",
      t7.contract_month as "単独オプション契約月数",
      t8.interior_color_id as "内装色ID",
      t8.interior_color_code as "内装色コード",
      t8.interior_color_display_name as "内装色表示名",
      t8.interior_upholstery_code as "内装色内張コード",
      t8.seat_color_display_name as "シート表示名",
      t8.recommended_interior_color_icon_code as "おすすめ内装色アイコンコード",
      t9.various_price_sv as "内装色_契約月数別変動金額SV",
      t10.price as "内装色_月額",
    
      t11.outer_plate_color_id as "外板色ID",
      t11.outer_plate_color_code as "外板色コード",
      t11.outer_plate_color_display_name as "外板色表示名",
      t11.recommended_outer_plate_color_icon_code as "おすすめ外板色アイコンコード",
      t11.outer_plate_color_additional_monthly_charge_tax_exclude_id as "外板色追加月額料金ID",
      t12.various_price_sv as "外板色_契約月数別変動金額SV",
      t13.price as "外板色_月額"
    
    from
      prod_kintojpn_view.view_rdb_contract as t1 /* 契約 */
      left outer join prod_kintojpn_view.view_rdb_based_car as t2 on /* 車基本情報 */
        t2.contract_id = t1.contract_id
      left outer join prod_kintojpn_view.view_rdb_car_contract_limited as t3 on /* 車契約情報 */
        t3.based_car_id = t2.based_car_id
      left outer join prod_kintojpn_view.view_rdb_equipment_limited as t4 on /* 装備情報 */
        t4.based_car_id = t2.based_car_id
      left outer join prod_kintojpn_view.view_rdb_m_single_option_detail as t5 on /* 単独オプション詳細マスタ(機密情報含む) */
        t5.single_option_id = t4.equipment_id
      left outer join prod_kintojpn_view.view_rdb_various_price as t6 on /* 契約月数別変動金額 */
        t6.various_price_id = t5.single_option_month_fee_id
      left outer join prod_kintojpn_view.view_rdb_various_price_detail as t7 on /* 契約月数別変動金額詳細 */
        t7.various_price_id = t6.various_price_id and t7.contract_month = t1.contract_months
    
      left outer join prod_kintojpn_view.view_rdb_m_interior_color as t8 on /* 内装色マスタ(機密情報含む) */
        t8.interior_color_id = t4.equipment_id
      left outer join prod_kintojpn_view.view_rdb_various_price as t9 on /* 契約月数別変動金額 */
        t9.various_price_id = t8.interior_color_additional_monthly_charge_tax_exclude_id
      left outer join prod_kintojpn_view.view_rdb_various_price_detail as t10 on /* 契約月数別変動金額詳細 */
        t10.various_price_id = t9.various_price_id and t10.contract_month = t1.contract_months
    
      left outer join prod_kintojpn_view.view_rdb_m_outer_plate_color as t11 on /* 外板色マスタ(機密情報含む) */
        t11.outer_plate_color_id = t4.equipment_id
      left outer join prod_kintojpn_view.view_rdb_various_price as t12 on /* 契約月数別変動金額 */
        t12.various_price_id = t11.outer_plate_color_additional_monthly_charge_tax_exclude_id
      left outer join prod_kintojpn_view.view_rdb_various_price_detail as t13 on /* 契約月数別変動金額詳細 */
        t13.various_price_id = t12.various_price_id and t13.contract_month = t1.contract_months
),

-- car_data_dupl_groupby 
car_data_dupl_groupby as
(
    select
      t1."契約ID",
    
      count(distinct t1."装備ID") as "装備ID数",
      count(distinct t1."単独オプションコード") as "単独オプション数",
      MAX(case when t1."単独オプショングループコード" = 'COLDAR' then 1 else 0 end) AS "単独オプグループ_雪寒地対応",
      MAX(case when t1."単独オプショングループコード" = 'COMOPT' then 1 else 0 end) AS "単独オプグループ_快適･利便性向上",
      MAX(case when t1."単独オプショングループコード" = 'IEXOPT' then 1 else 0 end) AS "単独オプグループ_内外装向上",
      MAX(case when t1."単独オプショングループコード" = 'OPTGRPZZZ' then 1 else 0 end) AS "単独オプグループ_あり",
      MAX(case when t1."単独オプショングループコード" = 'PKG001' then 1 else 0 end) AS "単独オプグループ_COLDAR",
      SUM( t1."単独オプション月額") as "単独オプション月額_合計",
      AVG( t1."単独オプション月額") as "単独オプション月額_平均",
      
      MAX(t1."内装色ID") as "内装色ID",
      MAX(t1."内装色コード") as "内装色コード",
      MAX(t1."内装色表示名") as "内装色表示名",
      MAX(t1."内装色内張コード") as "内装色内張コード",
      MAX(t1."シート表示名") as "シート表示名",
      MAX(t1."おすすめ内装色アイコンコード") as "おすすめ内装色アイコンコード",
      MAX(t1."内装色_契約月数別変動金額SV") as "内装色_契約月数別変動金額SV",
      MAX(t1."内装色_月額") as "内装色_月額",

      MAX(t1."外板色ID") as "外板色ID",
      MAX(t1."外板色コード") as "外板色コード",
      MAX(t1."外板色表示名") as "外板色表示名",
      MAX(t1."おすすめ外板色アイコンコード") as "おすすめ外板色アイコンコード",
      MAX(t1."外板色_契約月数別変動金額SV") as "外板色_契約月数別変動金額SV",
      MAX(t1."外板色_月額") as "外板色_月額"
    
    from
      car_data_dupl_origin as t1
    group by
      1
),

car_data_merge as
(
    select
      t1.*,
      t2.*
    from
        car_data_single_origin as t1
        left outer join car_data_dupl_groupby as t2 on
            t2."契約ID" = t1."契約ID"
    -- where
    --   t1."本契約申込日時(契約日)" between Timestamp '2024-01-01' and Timestamp '2024-01-31'

)

-- まとめ
select 
    t1.*
from 
    car_data_merge as t1
    -- car_data_origin as t1