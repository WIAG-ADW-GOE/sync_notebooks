SELECT r.id, r.name, r.factgrid_id FROM role as r 
LEFT JOIN (
    select * from url_external where authority_id = 42
) u on r.id = u.item_id ORDER BY r.id;