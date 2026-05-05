select distinct
    product_id,
    product_category_name_english as category,
    product_weight_g as weight_g,
    product_length_cm as length_cm,
    product_height_cm as height_cm,
    product_width_cm as width_cm,

    round(
        product_length_cm * product_height_cm * product_width_cm,
        2
    ) as volume_cm3

from {{ ref('stg_products') }}