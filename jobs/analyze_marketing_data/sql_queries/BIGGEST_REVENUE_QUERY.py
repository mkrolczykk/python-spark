biggest_revenue_query = (""
              "SELECT campaignId, round(AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x), 2) as revenue "
              "FROM ("
                  "SELECT campaignId, collect_list(billingCost) as billings "
                  "FROM (SELECT * "
                            "FROM target_dataframe "
                            "WHERE target_dataframe.isConfirmed == TRUE) "
                  "GROUP BY campaignId) "
              "ORDER BY revenue DESC LIMIT 10")