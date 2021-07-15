convert_array_of_maps_to_map = 'aggregate(slice(temp, 2, size(temp)), temp[0], (acc, element) -> map_concat(acc, element))'
