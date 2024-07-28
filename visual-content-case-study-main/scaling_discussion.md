### Scaling

* Memory Issue: If the input files are too big to fit in memory. One thing that can be done is to divide them into manageble batches before reading it. For example in images.jsonl you can preprocess and divide the input file such that a group of hotels and all it's associated images will come under one subfile. No of subfiles can be calculated according to memory management. We can now process the individual sub files separately. 

* The above solution will also be helpful when we join with the image_tags with images. For Tags subfiling we can sort it with image_id for faster key search.

* Considering that each hotels will have lot of images, processing each of the subfile will also require larger pods. I think going with less no of larger pods will be efficient.

