SELECT * FROM tbl_media_objects WHERE orig_name = '2018-02-10-0006833.jpg'

SELECT * FROM tbl_media_objects WHERE new_name = '2018-02-10-0006833.jpg'

SELECT * FROM image_tensors WHERE filename = '2019-09-20-0008910.jpg'


SELECT new_name FROM tbl_media_objects WHERE media_type = 'image' AND image_tensor_id IS NULL


UPDATE tbl_media_objects mo
SET image_tensor_id = it.id
FROM image_tensors it
WHERE mo.new_name = substring(it.filename from 10) -- Adjusting for "M:\Images\"
  AND mo.image_tensor_id IS NULL;

SELECT mo.media_object_id, mo.new_name, it.filename, TRIM(BOTH ' ' FROM substring(it.filename FROM 10)) AS extracted_filename
FROM tbl_media_objects mo
JOIN image_tensors it ON mo.new_name = TRIM(BOTH ' ' FROM substring(it.filename FROM 10))
WHERE mo.media_type = 'image' AND mo.image_tensor_id IS NULL
LIMIT 10;

SELECT mo.media_object_id, mo.new_name, it.filename, TRIM(BOTH ' ' FROM substring(it.filename FROM 10)) AS extracted_filename
FROM tbl_media_objects mo
JOIN image_tensors it ON TRIM(BOTH ' ' FROM mo.new_name) = TRIM(BOTH ' ' FROM substring(it.filename FROM 10))
WHERE mo.media_type = 'image' AND mo.image_tensor_id IS NULL
LIMIT 10;