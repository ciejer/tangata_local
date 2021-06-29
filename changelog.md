#Change Log

## 0.1.19
- Added left click context menus to tests & promotions, with tooltips
- Added Promoted Models to catalog landing page.

## 0.1.18
- Fixed issue #45: initial run was breaking on pip upgrade, where new config options were not found in `tangata_config.json`
- Resolved #49: now has config for +tags in dbt_project.yml. Behaviour respects existing tags where they exist, but all new keys will use configured choice.
- Resolved #48: now uses `preserve_quotes` for all `ruamel.yaml` calls.