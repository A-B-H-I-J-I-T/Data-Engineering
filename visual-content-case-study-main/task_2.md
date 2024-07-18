## Task 2: Implement the Processing Logic

Keeping in mind the scenario mentioned in [Task 1](task_1.md), you are now tasked with implementing a simplified version of the main image selection logic as a locally runnable (executable binary, script, docker, etc.) application in a suitable language of your choice (Python is preferred). However, **you are expected to use a big data processing framework such as Apache Spark or Apache Beam for this purpose**.

The selection logic is explained in the later part of this document.

Additionally, the input file may be too large to completely fit into memory and the records may still not adhere to the schema provided.

Your application must accept the following CLI parameters:

* `--images`<br>
    Local filesystem path to the JSONL file containing the images.
* `--tags`<br>
    Local filesystem path to the JSONL file containing the image tags.
* `--main_images`<br>
    Local filesystem path to the JSONL file containing the existing main images.
* `--output_cdc`<br>
    Local filesystem path to write JSONL file(s) containing the changes.
* `--output_snapshot`<br>
    Local filesystem path to write JSONL file(s) containing the snapshot of the pipeline run.
* `--output_metrics`<br>
    Local filesystem path to a single JSONL file to write the metrics of the pipeline.

These are **mandatory** CLI parameters and will be used to test your solution.

All schemas for inputs and outputs can be found inside the [schemas](schemas/) directory.

While this task is only supposed to work on a single machine, make sure it can make use of a modern machine with multiple CPU cores, if your language of choice allows it.

Also, briefly discuss how you would scale the code you have written across multiple machines if the need were to arise. **Don’t mistake this for a requirement to implement a solution that can be run in a distributed way**, just discuss the possibility. Remember to write your answer in the provided [scaling_discussion.md](scaling_discussion.md) document and to discuss the possibilities for the code you have written.

After completion of the main image selection logic, you should test your application to ensure it works as expected. We, too, will be conducting tests on your application. Therefore, please modify the existing [Makefile](Makefile) and adjust the run command accordingly. This will enable us to execute your application using the `make run` command. Remember, it’s mandatory to pass the necessary flags during this process.

## Logic

**Algorithm** : Main Image Selection Model  
**Input**     : Images data of width, height, created date and image tags.  
**Output**    : Highest scored image per item   

<pre>
<code>Initialise constant variables, including min & max resolution, aspect ratio and freshness, list of tags and their scores, and variable weights.

MIN_RES = 160000
MAX_RES = 2073600

MIN_AR = 0.3
MAX_AR = 4.65

MAX_FRESHNESS_DAY = 10 * 365

WEIGHT_RESOLUTION = 6
WEIGHT_ASPECT_RATIO = 2
WEIGHT_FRESHNESS = 2
WEIGHT_TAG_PRIORITY = 3

For each item<sub>t</sub>:
  Get data of the images which include width, height, created date and tags.
  For each image<sub>i</sub> in the item<sub>t</sub>:
    # Prepare features.

    Resolution<sub>i</sub> ¬ width<sub>i</sub> * height<sub>i</sub>.
    If height<sub>i</sub> = 0:
      AR<sub>i</sub> ¬ None
    Else:
      AR<sub>i</sub> ¬ width<sub>i</sub> / height<sub>i</sub>.
    End If

    If no accepted tag:
      Highest_tag_id<sub>i</sub> ¬ None
    Else:
      Highest_tag_id<sub>i</sub> ¬ accepted tag with highest probability
    End If

    # Calculate score of resolution.
    If Resolution<sub>i</sub> <= 0:
       Score_res<sub>i</sub> ¬ 0
    Else:
        Score_res<sub>i</sub> ¬ (log(Resolution<sub>i</sub>) - log(MIN_RES)) / (log(MAX_RES) - log(MIN_RES))
        If Score_res<sub>i</sub> > 1:
            Score_res<sub>i</sub> ¬ 1
    End If
    
    # Calculate score of AR.
    If AR<sub>i</sub> is None:
       Score_AR<sub>i</sub> ¬ 0
    Else:
      Score_AR<sub>i</sub> ¬ AR scoring rules
    End If
    
    # Calculate score of freshness.
    Score_fresh<sub>i</sub> ¬ 1 + (-1 * (today – created_date) / MAX_FRESHNESS)
    If Score_fresh<sub>i</sub> < 0:
        Score_fresh<sub>i</sub> ¬ 0
    Elif Score_fresh<sub>i</sub> > 1:
        Score_fresh<sub>i</sub> ¬ 1
    End If
  
    # Calculate score of tag priority.
    Score_tagi ¬ Tag priority scoring rules
  
    # Calculate score of each image.
    Score_image<sub>i</sub> ¬ (
                        WEIGHT_RESOLUTION * Score_res<sub>i</sub> + 
                        WEIGHT_ASPECT_RATIO * Score_AR<sub>i</sub> + 
                        WEIGHT_FRESHNESS * Score_fresh<sub>i</sub> + 
                        WEIGHT_TAG_PRIORITY * Score_tagi
                    )/(
                        WEIGHT_RESOLUTION + WEIGHT_ASPECT_RATIO + 
                        WEIGHT_FRESHNESS + WEIGHT_TAG_PRIORITY
                    )
    Rank images DESC by Score_image<sub>i</sub>
    Assign the 1<sup>st</sup> ranked image as the item main image.
End
</code>
</pre>