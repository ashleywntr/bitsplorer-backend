$date = (Get-Date).AddDays(-1) # Get previous days' date
$format_date = $date.ToString("yyyy-MM-dd")
(New-Object Net.WebClient).DownloadString("http://py-chain.ddns.net:55397/api/blockdays?date=$format_date")

