// Microsoft UAL event_type → plain English label.
// Every surface in the UI that shows an event type should go through
// getEventLabel() so the operator never sees a raw API string. The raw
// value is still available in the expanded raw_json view.

const EVENT_LABELS = {
  // ----- Exchange / Email -------------------------------------------------
  "MailItemsAccessed":                  "Email Accessed",
  "FolderBind":                         "Mailbox Opened",
  "Send":                               "Email Sent",
  "SendAs":                             "Email Sent As",
  "SendOnBehalf":                       "Email Sent On Behalf",
  "HardDelete":                         "Email Permanently Deleted",
  "SoftDelete":                         "Email Deleted",
  "Move":                               "Email Moved",
  "MoveToDeletedItems":                 "Moved to Trash",
  "UpdateInboxRules":                   "Inbox Rule Changed",
  "UpdateCalendarDelegation":           "Calendar Access Changed",
  "SetMailboxCalendarConfiguration":    "Calendar Settings Changed",
  "UpdateFolderPermissions":            "Folder Permissions Changed",
  "MailboxLogin":                       "Mailbox Login",

  // ----- SharePoint / OneDrive --------------------------------------------
  "FileAccessed":                       "File Viewed",
  "FileAccessedExtended":               "File Viewed (Extended)",
  "FileModified":                       "File Modified",
  "FileModifiedExtended":               "File Modified (Extended)",
  "FileDeleted":                        "File Deleted",
  "FileDownloaded":                     "File Downloaded",
  "FileDownloadedFromBrowser":          "File Downloaded (Browser)",
  "FileRenamed":                        "File Renamed",
  "FilePreviewed":                      "File Previewed",
  "FileCreated":                        "File Created",
  "FileSyncUploadedFull":               "File Synced (Upload)",
  "FileSyncDownloadedFull":             "File Synced (Download)",
  "FileCreatedOnRemovableMedia":        "File Copied to USB Drive",
  "AnonymousLinkUsed":                  "Anonymous Link Used",
  "SharingLinkUsed":                    "Sharing Link Used",
  "SharingSet":                         "File Shared",
  "PageViewed":                         "Page Viewed",
  "ListViewed":                         "List Viewed",
  "ListItemViewed":                     "List Item Viewed",

  // ----- Azure AD / Identity ----------------------------------------------
  "UserLoggedIn":                       "User Sign-In",
  "UserLoginFailed":                    "Failed Sign-In",
  "UserLoggedOut":                      "User Sign-Out",
  "Add member to group.":               "Added to Group",
  "Remove member from group.":          "Removed from Group",
  "Update user.":                       "User Updated",
  "Reset user password.":               "Password Reset",
  "Change user password.":              "Password Changed",
  "Set force change user password.":    "Force Password Change",
  "Consent to application.":            "App Access Granted",
  "Add app role assignment grant to user.": "App Role Assigned",
  "Add service principal.":             "Service Principal Added",

  // ----- Teams ------------------------------------------------------------
  "TeamsSessionStarted":                "Teams Session Started",
  "MessageCreatedHasLink":              "Teams Message Sent (Link)",
  "MeetingParticipantDetail":           "Joined Meeting",

  // ----- General ----------------------------------------------------------
  "TaskListRead":                       "Task List Viewed",
  "TIMailData":                         "Threat Intel Mail Event",
};

// Unmapped events fall through to a CamelCase -> spaced-words split so they
// still render as something readable. Unknown input that already contains
// spaces (e.g. already a label) is returned untouched.
export function getEventLabel(eventType) {
  if (eventType === null || eventType === undefined) return "";
  const raw = String(eventType);
  if (EVENT_LABELS[raw]) return EVENT_LABELS[raw];
  // Already-humanized strings (contain a space) pass through.
  if (/\s/.test(raw)) return raw;
  return raw.replace(/([A-Z])/g, " $1").trim();
}
