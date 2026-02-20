# Azure Tips and Tricks

The following section provides useful suggestions, searches, actions, hints, and prompts to help with navigating and working with Azure, the Azure CLI, and portal.azure.com. This is a living document that will continue to be updated as we expand our cloud journey.

## Table of Contents
- [View Resource Groups](#view-resource-groups)
- [View Event Details](#view-event-details)
- [Create a Dashboard in Azure](#create-a-dashboard-in-azure)

---

## View Resource Groups

To see resource groups in the Azure portal:

1. Go to the resource group service page: https://portal.azure.com/#view/HubsExtension/BrowseResourceGroups
2. Click `Add filter`
3. Filter on `Admin`
4. Set the Value to `ALL` or select any names you are interested in

**Note**: The names in the list are the only ones that have the Admin tag set. You can also specify to filter on other fields from there.

---

## View Event Details

To find who initiated an event or view activity logs:

1. On the left-hand panel when viewing a resource group, select `Activity Log`
2. Select `Add filter` → `Event Initiated by`
3. Adjust the timespan filter as necessary to view the event

**Tip**: You can optionally add an `Event category = Administrative` filter for more specific results.

---

## Create a Dashboard in Azure

To create a custom dashboard in Azure:

1. Login to the Azure portal
2. Click on the `hamburger icon` (located at the top left corner of the page)
3. Select `Dashboard` to go to your dashboards
4. Click `Create` (top left corner)
5. Click on the `Custom` tile
6. Edit the input box and provide a name for your dashboard
7. Click on `Resource groups` in the tile gallery
8. Click `Add`
9. Click and drag the `lower right corner` of the tile to resize it to your liking
10. Click `Save` to save your settings

You should now be taken to your new dashboard.

### Configure Dashboard Filters

To filter your dashboard by admin/owner:

1. Click on your new dashboard tile
2. Click on `Add filter` (located at the top center of the page)
3. Click on the `Filter` input box to reveal the tags
4. Select the `Admin` tag
5. Click on the `Value` input box
6. Click on `Select all` to unselect all
7. Select your name
8. Click on `Apply`

### Save Dashboard View

To save your customized dashboard view:

1. Click on `Manage view` (located at the top left of the page)
2. Select `Save view`
3. Enter a name for the view in the input box
4. Click `Save`

---

*This document will be updated regularly with additional tips and tricks as we continue our Azure journey.*