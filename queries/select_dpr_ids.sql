SELECT persons.factgrid, persons.id, gsn.nummer
FROM items
INNER JOIN persons ON persons.item_id = items.id AND persons.deleted=0 AND items.deleted=0 AND items.status = "online" 
INNER JOIN gsn ON gsn.item_id = items.id AND gsn.deleted=0