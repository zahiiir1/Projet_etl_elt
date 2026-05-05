with orders as (

    select *
    from {{ ref('stg_orders') }}

),

payments_agg as (

    select
        order_id,
        round(sum(payment_value), 2) as montant_total,
        max(payment_sequential) as nb_paiements,
        max(payment_installments) as nb_versements

    from {{ ref('stg_order_payments') }}
    group by order_id

),

main_payment as (

    select
        order_id,
        payment_type as type_paiement

    from (
        select
            order_id,
            payment_type,
            payment_value,
            row_number() over (
                partition by order_id
                order by payment_value desc
            ) as rn
        from {{ ref('stg_order_payments') }}
    )
    where rn = 1

),

reviews_agg as (

    select
        order_id,
        round(avg(review_score), 2) as review_score,
        count(review_id) as nb_reviews,
        max(
            case
                when trim(review_comment_message) <> '' then 1
                else 0
            end
        ) as a_commente

    from {{ ref('stg_order_reviews') }}
    group by order_id

),

final as (

    select
        o.order_id,
        o.customer_id,

        cast(strftime(cast(o.order_purchase_timestamp as date), '%Y%m%d') as integer) as date_id,

        o.order_status as status,

        p.montant_total,
        p.nb_versements,
        p2.type_paiement,
        p.nb_paiements,

        date_diff(
            'day',
            cast(o.order_purchase_timestamp as date),
            cast(o.order_delivered_customer_date as date)
        ) as delai_livraison_jours,

        date_diff(
            'day',
            cast(o.order_delivered_customer_date as date),
            cast(o.order_estimated_delivery_date as date)
        ) as livraison_en_avance,

        case
            when o.order_delivered_customer_date is null then 'non livre'
            when date_diff(
                'day',
                cast(o.order_delivered_customer_date as date),
                cast(o.order_estimated_delivery_date as date)
            ) > 0 then 'en avance'
            when date_diff(
                'day',
                cast(o.order_delivered_customer_date as date),
                cast(o.order_estimated_delivery_date as date)
            ) = 0 then 'a temps'
            else 'en retard'
        end as statut_livraison,

        case
            when o.order_delivered_customer_date is not null
                 and date_diff(
                    'day',
                    cast(o.order_purchase_timestamp as date),
                    cast(o.order_delivered_customer_date as date)
                 ) < 7
            then 1
            else 0
        end as livraison_rapide,

        extract(year from o.order_purchase_timestamp) as annee_achat,
        extract(month from o.order_purchase_timestamp) as mois_achat,
        extract(quarter from o.order_purchase_timestamp) as trimestre_achat,
        strftime(o.order_purchase_timestamp, '%A') as jour_semaine_achat,

        r.review_score,

        case
            when r.review_score is null then 'inconnu'
            when r.review_score >= 4 then 'positif'
            when r.review_score = 3 then 'neutre'
            else 'negatif'
        end as satisfaction,

        r.a_commente,
        r.nb_reviews

    from orders o
    left join payments_agg p
        on o.order_id = p.order_id
    left join main_payment p2
        on o.order_id = p2.order_id
    left join reviews_agg r
        on o.order_id = r.order_id

)

select
    order_id,
    customer_id,
    date_id,
    status,
    montant_total,
    nb_versements,
    type_paiement,
    nb_paiements,
    delai_livraison_jours,
    livraison_en_avance,
    statut_livraison,
    livraison_rapide,
    annee_achat,
    mois_achat,
    trimestre_achat,
    jour_semaine_achat,
    review_score,
    satisfaction,
    a_commente,
    nb_reviews

from final