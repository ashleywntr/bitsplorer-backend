$date = (Get-Date).AddDays(-1)
$format_date = $date.ToString("yyyy-MM-dd")
(New-Object Net.WebClient).DownloadString("http://192.168.0.18:55397/api/blockdays?date=$format_date")

