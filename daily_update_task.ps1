$date = (Get-Date).AddDays(-1)
$format_date = $date.ToString("yyyy-MM-dd")
(New-Object Net.WebClient).DownloadString("http://192.168.1.194:55397/api/blockdays?date=$format_date")

