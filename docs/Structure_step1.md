The goal of step 1 is to have a database of all the files in the BDRC image archive. BDRC has a database of about 50 million images, over 300TB of data, accumulated over 25 years.

Step 1 has its own benefits (the image database) and also prepares for step 2, which consists in a migration to Oxford Common File Layout ([OCFL](https://ocfl.io/)).

The current file structure of the image archive is the following:

Leading to the object folders:

`{parent}/{root}/{object_id_end}/{object_id}/`

where 
- `{object_id}` is the *image instance local name*, starting with W, ex: `W22084`, we call it object as it will be transformed into ocfl objects in step 2. The object contains all the files for a set of scans
- `{object_id_end}` is composed of the last 2 characters of the `object_id` (ex: `84` for `W22084`), or `00` of the last two characters are not decimal digits
- `{root}` is `Archive0`, `Archive1`, `Archive2` or `Archive3`. `Archive0` contains all the folders where `object_id_end` is between `00` and `24`, `Archive2` between `25` and `49`, etc.
- `{parent}` is the mounting point, configurable in the script, `/mnt` by default.

The object folders are organized in the following way:
- `images/{object_id}-{volume_folder_id}/{image_file}` (mandatory, structure enforced)
- `archive/{object_id}-{volume_folder_id}/{archive_file}` (optional, structure not systematically enforced)
- `sources/` (optional, free organization)
- any other directory, theoretically to ignore but can sometimes contain important data, especially for early folders

`{volume_folder_id}` have two possible forms:
- starting with `I`, they represent the `volume_id` (ex: `I3KG765`)
- composed of four digits, they represent the `volume_id` without the initial `I`, ex: folder `0987` is volume_id `I0987`

`{image_file}` is theoretically always in the form `{volume_folder_id}{image_num}.{extension}` where `{image_num}` is 4 digits and `{extension}` is either `jpg` or `tif`. This was not historically enforced and can have exceptions.

`{archive_file}` is theoretically always in the form `{anything}{image_num}.{extension}` where `{image_num}` is 4 digits and corresponds to an existing image_num in the `images/` folder. This was not historically enforced and can have exceptions.

