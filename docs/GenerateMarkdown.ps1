$inputFile = ".\Getting Started.docx"
$outputFile = ".\Getting Started.md"

pandoc -s --extract-media ./images/GettingStarted $inputFile -t gfm -o $outputFile

$content = Get-Content -Path $outputFile -Raw

$find = 'Inline_code'
$replace = "``````"

$updatedContent = $content -replace $find, $replace

Set-Content -Path $outputFile -Value $updatedContent -Force