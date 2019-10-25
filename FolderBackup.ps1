########################################################
# Name: FolderBackup.ps1                              
# Creator: Ruslan Nour                    
# CreationDate: 05.10.2019                              
# LastModified: 05.10.2019                               
# Version: 1.0
# 
#
# Description: Copies the Bakupdir to the Destination
# 

#Variables, only Change here
$DestinationRoot="D:\backup\" #Copy the Files to this Location
$BackupDirs="C:\Users\U\Documents\LEGO Creations" #What Folders you want to backup

### Make a backup of the Backup folder
$date = Get-Date -Format FileDate
$DestinationDir = $DestinationRoot + "\" + $date
mkdir $DestinationDir
copy-item $BackupDirs $DestinationDir -recurse