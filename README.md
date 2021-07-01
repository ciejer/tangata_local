# tāngata_local
>“Hutia te rito o te harakeke  
Kei whea to kōmako e kō?  
Ki mai ki ahau  
He aha te mea nui o te Ao?  
Maku e kī atu,  
he tāngata, he tāngata, he tāngata..."

If the heart of the harakeke *(flax plant)* was removed,  
where would the bellbird sing?  
If I was asked what was the most important thing in the world  
I would be compelled to reply,  
it is people, it is people, it is people.  
*Ngaroto*

In te ao Māori (the Māori world view), Tāngata (*TAHNG-uh-tuh*) describes something much larger than an addressed group of people: it describes *whakapapa*, the surrounding network of ancestors and descendants we are connected to.  
With this work we intend to follow these principles to put our people first: not just the data & analytics engineers, but those around our workplaces that know the deep details of how our businesses actually run.  
These people are the lifeblood of what we do - and to keep moving forward, we need their context far more than ever.

## Current Functionality
Tāngata is an editable Data Catalog, describing a dbt_ repository.  
It interfaces with dbt_ itself, git, and other sources to compile metadata in one place; and allows a non-technical user to understand what's been built, and contribute metadata to the sources & models within.  
With descriptive metadata, edit history, lineage, and SQL code all available in one place, this should become the default search engine of an organisation's data users; and with specific attention applied to runtime speed will be an enjoyable place to work regardless of the technical background of any user.

The complexity of Git and engineering practise in general can be difficult to approach. With Tāngata, this sits behind the scenes - giving comfort to those who are important, while maintaining a strong, secure foundation for our most critical metadata.

## Future Functionality
This project started as a graphical SQL interface - and still contains some of the pieces behind the scenes.  
While the pivot to editable catalog has taken over for some time, the dream is still bigger: what if a non-technical user could design a straightforward dbt_ model using just drag and drop?  
This approach may take some time, but not out of reach - SQL is structured by definition, it just takes the right interface.

## Installation Instructions

`pip install tangata`

## To Serve Tāngata:

- Navigate to your dbt project folder
- Run `./tangata [--skipcompile]`
- Tangata will be served at http://localhost:8080

## Where's the React code?

This project was initially created as a npm-backed React app. The Javascript/React code for the front end is still in that repository as Python only needs the static output; the front end portion will be moved to a standalone repo at some point.

## I have an idea!

Please log all feedback in the GitHub Issues - your feedback is crucial to make this a useful tool for the community.

#Change Log

## 0.2.0
- Added config option to sort .yml files alphabetically
- Fixed model ordering issue in db tree view
## 0.1.19
- Added left click context menus to tests & promotions, with tooltips
- Added Promoted Models to catalog landing page.

## 0.1.18
- Fixed issue #45: initial run was breaking on pip upgrade, where new config options were not found in `tangata_config.json`
- Resolved #49: now has config for +tags in dbt_project.yml. Behaviour respects existing tags where they exist, but all new keys will use configured choice.
- Resolved #48: now uses `preserve_quotes` for all `ruamel.yaml` calls.
