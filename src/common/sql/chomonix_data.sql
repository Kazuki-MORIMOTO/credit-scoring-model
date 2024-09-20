select
  t1.member_id as "メンバーID",
  t3.postcode as "郵便番号",
  t3.city as "市区郡町村",
  t7.col_2 as "ソーシャルグループ",
  t7.col_3 as "都市化度",
  t7.col_5 as "クラスタ名",
  t7.col_6 as "特徴"
from
  prod_kintojpn_view.view_rdb_member as t1 /* 会員情報 */
  inner join prod_kintojpn_view.view_rdb_member_personal as t2 on /* 会員個人情報 */
    t2.member_id = t1.member_id
  inner join prod_kintojpn_view.view_rdb_contact_address as t3 on /* 連絡先情報 */
    t2.contact_address_id = t3.contact_address_id
  inner join prod_kintojpn_view.view_chomonicx40_summary as t6 on /* Chomonicx加工済みデータ（郵便番号サマリ） */
    t3.postcode = t6.postal_code_hyphen
  inner join prod_nicola_user.V_0002H0MDC7HRCNVSHRY5F15CY5 as t7 on /* chomonicxクラスター名と特徴 */
    t6.chomonicx40_code = t7.col_1
group by
  t1.member_id,
  t3.postcode,
  t3.city,
  t7.col_2,
  t7.col_3,
  t7.col_5,
  t7.col_6
order by
  t1.member_id,
  t3.postcode,
  t3.city,
  t7.col_2,
  t7.col_3,
  t7.col_5,
  t7.col_6
