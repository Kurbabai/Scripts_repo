$folder1 = "\\rv-qa-sdnas-01\nas\WX\Download\GEFS\20190227\00"
$folder2 = "\\rv-qa-sdnas-01\nas\WX\Download\GEFS\20190227\06"
$folder3 = "\\rv-qa-sdnas-01\nas\WX\Download\GEFS\20190227\12"
$folder4 = "\\rv-qa-sdnas-01\nas\WX\Download\GEFS\20190227\18"
$folders = @($folder1,$folder2,$folder3,$folder4)
$correct_amount = 8000

$EmailFrom = "notifications@somedomain.com"
$EmailTo = "idanad@gmail.com" 
$Subject = "Houston we have a problem" 
$SMTPServer = "smtp.gmail.com" 
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587) 
$SMTPClient.EnableSsl = $true 
$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("idanad", "some_password"); 

foreach ($folder in $folders) {
    $current_amount = ( Get-ChildItem $folder | Measure-Object ).Count
    if ( $current_amount -ne $correct_amount ) {
        $Body = "This folder $folder has incorrect amount of files `n It has $current_amount files" 
        $SMTPClient.Send($EmailFrom, $EmailTo, $Subject, $Body)
    }
}