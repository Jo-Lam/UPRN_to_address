# UPRN
Link UPRN to Addresses
Aim: Link UPRN to individual addresses

Resource:
1) Ordinance Survey Open UPRN (https://osdatahub.os.uk/downloads/open/OpenUPRN)
- This release provides UPRNs, X-coordinate, Y-coordinate (British Grid), Latitute, Longitude
- read more at: https://www.ordnancesurvey.co.uk/documents/product-support/tech-spec/open-uprn-techspec-v1.pdf

2) Nominatim reverse (https://nominatim.openstreetmap.org/ui/reverse.html)
- from lat and long, nominatim outputs address information.

This is a simple python function that sends request to Nominatim, using Lat/Long info from Open UPRN,
and outputs a csv that allows matching of UPRN with Addresses.


