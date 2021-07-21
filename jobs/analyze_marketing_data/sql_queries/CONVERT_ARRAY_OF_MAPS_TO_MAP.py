convert_array_of_maps_to_map = 'aggregate(slice(campaign, 2, size(campaign)), campaign[0], (acc, element) -> map_concat(acc, element))'
