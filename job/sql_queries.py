biggest_revenue_query = (""
              "SELECT campaignId, round(AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x), 2) as revenue "
              "FROM ("
                  "SELECT campaignId, collect_list(billingCost) as billings "
                  "FROM (SELECT * "
                            "FROM target_dataframe "
                            "WHERE target_dataframe.isConfirmed == TRUE) "
                  "GROUP BY campaignId) "
              "ORDER BY revenue DESC LIMIT 10")

most_popular_channel_query = (""
                "SELECT campaignId, channelIid, unique_sessions "
                "FROM ("
                    "SELECT campaignId, channelIid, unique_sessions, ROW_NUMBER() OVER(PARTITION BY campaignId ORDER BY unique_sessions DESC) as rnk "
                    "FROM ("
                        "SELECT campaignId, channelIid, COUNT(*) as unique_sessions "
                        "FROM target_dataframe "
                        "GROUP BY campaignId, channelIid) "
                    ")"
                "WHERE rnk == 1"
                "")