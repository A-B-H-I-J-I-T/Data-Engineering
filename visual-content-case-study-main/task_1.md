
## Task 1: Design a Main Image Selection Pipeline

You are working in a hotel meta search and users can search for hotels or destinations to score better prices. In order to provide the best user experience, your team is tasked with selecting the best main image (thumbnail) for each hotel. As the first step, you should propose a solution using a [design document](design_document.md) for this purpose to cater the requirements that are mentioned below.


### Details 

You get images from different sources (partners, hotels, etc.) and each images belongs to a single hotel. The system has more than 10 million hotels and each hotel can have various number of images ranging from zero to tens of thousands. 

When images get imported, additional features get extracted. The following features are available for the the main image selection login to use if desired.

**1-Tags**

- Tags are labels representing the contents of the image such as `bedroom`, `bathroom`, `balcony`, etc. 
- An in-house Machine Learning model is used to predict tags along with the corresponding probabilities *(denoting the model's confidence)*.
- Each image is expected to have up to 10 tags.

In the following example, each image has multiple tags listed.

Example:

| Image ID    | Tag | Version | Probability |
| -------- | ------- | ------- | -------- |
| 1     | bedroom    | v4       |0.98 |
| 1     | bathroom   | v4       |0.91 |
| 2     | pool       | v4       |0.95 |
| 2     | bedroom    | v4       |0.01 |

**2- Quality Attributes**


Quality attributes of images include resolution, noise, sharpness, focus, contrast, etc. 
These values are also considered in the selection logic as **OB**jective **I**mage **Q**uality (`obiq`) values to represent the overall quality of the images.

For simplicity, you can assume that a computed value called `obiq_score` is available for each image to be read from another storage location. 
These values are calculated using the aforementioned attribute values for each image and the score will range from `0` to `1` where a higher score represents a better quality image.

Example:

| Image ID    | OBIQ Score | Version |
| -------- | ------- | ------- | 
| 1  | 0.28749    | v1 |
| 2 | 0.962352    | v1 |
| 2    | 0.65354    | v1 |


Even though you are not expected to come up with an equation to derive the `obiq_score`, feel free to explain how each `obiq_value` contribute to the quality of an image.


### Constraints

Design your solution under the following assumptions and constraints

* Images are downloaded in to a storage location as blobs and their metadata (location, source, etc.) are stored separately for easy access.
* The images can get frequently added or removed in the system.  
    * If an image gets deleted and it's already used as a main image, a new main image should be calculated.
* The images, image tags, obiq_scores, and existing main images are provided as an input to the system.
    * You are free to decide which type of storage (blob, database, etc.), hosting strategy (on-prem or cloud), and any tyoe of scaling if needed.
    * Make sure to explain these decisions in your design along with the corresponding schemas.
* The input only contains the image tags with higher probabilities.   
    These thresholds are defined outside of this pipeline and we can assume the tags in the input are already filtered to be useful for our processing.
* The pipeline should read the existing main images and only produce the changes (CDC).  
    * If the newly generated main image is the same as the current one, no update should be sent.
    * If there is no existing main image, but a new image is generated, then a new record should be sent.
    * If the newly selected main image is different than the current one, and update record should be sent.
* Sometimes, the tags and obiq scores for certain images come (even a couple of days) later than the image record.  
    Since the image processing is done asynchronously in separate pipelines, there is no guarantee that we will have all the information needed to calculate the best image. But we should still calculate a main image with the information at hand, and use the information when they're available.
*  The system must be scalable.  
    There is no control on how many images we will have as the input, and this pipeline must be able to handle them.
* The solution must be extendable.  
    * The system should be able to welcome new input (such as image quality attributes, user interactions, etc.) in the future. You don't have to implement it now, but the solution should easily be changed to read those.
    * The selection logic is developed by Data Scientists. When a new version of the logic comes, it should be easy to integrate to the pipeline and calculate main images using it.
* The solution must be fault-tolerant  
    If the pipeline crashes or doesn't run for a couple of days, the next sucessful run should still generate correct main images.
* There are two outputs generated from the pipeline.  
    * Changes - These are send to the front end team to update the main image accordingly.
    * Snapshot - This represent the current state of the main images at a given time.
* You are free to choose between a streaming or a batch solution.  

### Requirements

* Propose a design for the system.
* Reporting solution to get below metrics from the system to be delivered to the Product Department:
    * Number of images processed
    * Number of hotels with images
    * Number of main images
        * Newly elected
        * Updated
        * Deleted

### Optional Requirements
* There also is an API used by admins to set main images for hotels.   
    Our automated pipeline should not override human-set main images unless the current main image is deleted or deactivated. 

* We will have different model versions for calculating image tags and OBIQ scores.  
    Whenever we implement a new Machine Learning model or a scoring model, we introduce corresponding new versions and the system should be able to detect the new data and use those for processing. You may briefly explain the strategies we can use to maintain different versions of each of our models and how we can use them in our data pipelines.

### Design Document

Make sure your design document contains at least the following:

1. An architecture diagram of your proposed solution.
2. A diagram showing the flow of data through your solution.
3. A rough outline of your deployment and scaling strategies.