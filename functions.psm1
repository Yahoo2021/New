function GetInsertedRows {
	[CmdletBinding()]
	param(
		[Parameter()]
        [Object[]] $result,
		[string] $SinkName
	)

    $json = ($result.Output | Select-Object -First 1) -join "`r`n" | ConvertFrom-Json

    [int]$RowsWritten = ($json | Select-Object -ExpandProperty Metrics).$SinkName.RowsWritten 
    return $RowsWritten
}

function GetRowsCopied {
	[CmdletBinding()]
	param(
		[Parameter()]
        [Object[]] $result
	)
    
    return ('[{' + ($result.Output -join ",`r`n") + '}]'  | ConvertFrom-Json).rowsCopied
}

function GetPipelineDateParameter {
    $PipelineParameter = @{}
    $PipelineParameter.Add('LastModifiedFrom', (get-date).ToString(‘yyyy-MM-dd’))
    $PipelineParameter.Add('LastModifiedTo', ((get-date).AddDays(1)).ToString(‘yyyy-MM-dd’))

    return $PipelineParameter
}

function RunPipeline {
	[CmdletBinding()]
	param(
		[Parameter()]
        [string] $ResourceGroupName,
        [string] $DataFactoryName,
        [string] $PipelineName
	)
    
    $pollFrequency = 10

    $PipelineParameter = @{}
    $PipelineParameter.Add('LastModifiedFrom', (get-date).ToString(‘yyyy-MM-dd’))
    $PipelineParameter.Add('LastModifiedTo', ((get-date).AddDays(1)).ToString(‘yyyy-MM-dd’))
    
    #Start Pipeline
    $PipelineRunId = Invoke-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -PipelineName $PipelineName -Parameter $PipelineParameter

    #$PipelineRunId = "d9eaff85-1c61-487b-bfb8-2425122a759c"
    #Get run status
    $runStatus = (Get-AzDataFactoryV2PipelineRun -ResourceGroupName $resourceGroupName -DataFactoryName $DataFactoryName -PipelineRunId $PipelineRunId).Status
    
    #Write-Host ("Pipeline {0} in progress" -f $pipelineName) -NoNewline
    
    #Wait until pipeline is running
    While ($runStatus -eq 'InProgress') {

        #Write-Host "." -NoNewline #Low budget progress bar
        Start-Sleep -second $pollFrequency 

        $runStatus = (Get-AzDataFactoryV2PipelineRun -ResourceGroupName $resourceGroupName -DataFactoryName $DataFactoryName -PipelineRunId $PipelineRunId).Status
    }

    return $PipelineRunId
}

function CopyTestFiles{
	[CmdletBinding()]
	param(
		[Parameter()]
        [object] $context,
        [string] $Source,
        [string] $Destination,
        [string] $SourceContainer,
        [string] $DestinationContainer
	)
    $Paths = (Get-AzStorageBlob -Context $context -Container $SourceContainer -blob  $Source).Name

    [int]$NumberOfFiles = 0

    foreach($path in $paths){
        $FileName = Split-Path $Path -leaf
            
        $CopyResult = Start-AzStorageBlobCopy -Context $context -SrcBlob $path -DestBlob ($Destination + $FileName ) -SrcContainer $SourceContainer -DestContainer $DestinationContainer

        $NumberOfFiles += $CopyResult.count
    }
    return $NumberOfFiles
}

function TestToConformed {
	[CmdletBinding()]
	param(
		[Parameter()]
        [object] $ctx,
        [string] $SourceContainer,
        [string] $SourceFolder,
        [string] $DestinationContainer,
        [string] $DestinationFolder,
        [bool]   $DeleteDestination,
        [string] $TestFile,
        [string] $ResourceGroupName,
        [string] $DataFactoryName,
        [string] $PipelineName,
        [string] $SinkName
	)

    #Delete existing source files
    Get-AzStorageBlob -Context $ctx -Container $SourceContainer -blob ($SourceFolder + (get-date).ToString(‘yyyy/MM/dd’) + "/*.parquet") | Remove-AzStorageBlob -Force

    #Copy test data set to the source folder
    CopyTestFiles -context $ctx -Source $TestFile -Destination ($SourceFolder + (get-date).ToString(‘yyyy/MM/dd’) + '/') -SourceContainer $SourceContainer -DestinationContainer $SourceContainer


    if($DeleteDestination){
        Get-AzStorageBlob -Context $ctx -Container $DestinationContainer -blob ($DestinationFolder + "*.parquet") | Remove-AzStorageBlob -Force
    }


    $RunId = RunPipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -PipelineName $PipelineName
    $result = Get-AzDataFactoryV2ActivityRun -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName -PipelineRunId $RunId -RunStartedAfter (Get-Date).AddMinutes(-3000) -RunStartedBefore (Get-Date).AddMinutes(30)

    # GA_UserJourney copies file, so have to use different JSON property to get correct test results
    if($PipelineName -eq "PL_GA_UserJourneyToConformed"){
        [int]$InsertedRows = GetRowsCopied -result $result
    }
    else
    {
        [int]$InsertedRows = GetInsertedRows -result $result -SinkName $SinkName
    }
    #$InsertedRows | gm | Write-Host
    return $InsertedRows
}

function TestConformedToSQL {
	[CmdletBinding()]
	param(
		[Parameter()]
        [object] $ctx,
        [string] $SourceContainer,
        [string] $SourceFolder,
        [bool]   $DeleteSource,
        [string] $TestFile,
        [string] $ResourceGroupName,
        [string] $DataFactoryName,
        [string] $PipelineName
	)

  #  if($DeleteSource){
        #Delete existing source files
        Get-AzStorageBlob -Context $ctx -Container $SourceContainer -blob ($SourceFolder + (get-date).ToString(‘yyyy/MM/dd’) + "/*") | Remove-AzStorageBlob -Force
 #   }

        #Copy test data set to the source folder
        CopyTestFiles -context $ctx -Source $TestFile -Destination ($SourceFolder + (get-date).ToString(‘yyyy/MM/dd’) + '/') -SourceContainer $SourceContainer -DestinationContainer $SourceContainer


    #Run pipeline
    $RunId = RunPipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -PipelineName $PipelineName
    
	
    #Process result
    $result = Get-AzDataFactoryV2ActivityRun -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName -PipelineRunId $RunId -RunStartedAfter (Get-Date).AddMinutes(-3000) -RunStartedBefore (Get-Date).AddMinutes(30)
    
    [int]$InsertedRows = GetRowsCopied -result $result 

#$InsertedRows

    #[int]$InsertedRows =GetRowsWritten -result $result 

#Write-Output "Activity run details:"
#$Result

#Write-Output "Activity 'Output' section:"
#$Result.Output -join "`r`n"

#Write-Output "Activity 'Error' section:"
#$Result.Error -join "`r`n"

    #$InsertedRows | gm | Write-Host
    return $InsertedRows
}

function TestOnPremToRaw_SQL {
	[CmdletBinding()]
	param(
		[Parameter()]
        [string] $ResourceGroupName,
        [string] $DataFactoryName,
        [string] $PipelineName
	)

    #Run pipeline
    $RunId = RunPipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -PipelineName $PipelineName
    
    #Process result
    $result = Get-AzDataFactoryV2ActivityRun -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName -PipelineRunId $RunId -RunStartedAfter (Get-Date).AddMinutes(-3000) -RunStartedBefore (Get-Date).AddMinutes(30)
    
    [int]$InsertedRows = GetRowsCopied -result $result 
    #$InsertedRows | gm | Write-Host
    return $InsertedRows
}

function TestOnPremToRaw_FS {
	[CmdletBinding()]
	param(
		[Parameter()]
        [object] $ctx,
        [string] $FilePath,
        [string] $TestFileContainer,
        [string] $TestFile,
        [string] $ResourceGroupName,
        [string] $DataFactoryName,
        [string] $PipelineName
	)

    #Delete existing source files
    $DeletedFiles = Remove-Item ($FilePath + '*.csv')

    #Copy test data set to the source folder
    $blobs = Get-AzStorageBlob -Context $ctx -Container $TestFileContainer -blob $TestFile
    foreach($blobContent in $blobs)  
    {  
        ## Download the blob content  
        Get-AzStorageBlobContent -Container $TestFileContainer  -Context $ctx -Blob $blobContent.Name -Destination ($FilePath + (Split-Path $blobContent.Name -leaf)) -Force  
    } 

    #Run pipeline
    $RunId = RunPipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -PipelineName $PipelineName


    #Process result
    $result = Get-AzDataFactoryV2ActivityRun -DataFactoryName $DataFactoryName -ResourceGroupName $ResourceGroupName -PipelineRunId $RunId -RunStartedAfter (Get-Date).AddMinutes(-3000) -RunStartedBefore (Get-Date).AddMinutes(30)
    
    [int]$InsertedRows = GetRowsCopied -result $result 
    #$InsertedRows | gm | Write-Host
    return $InsertedRows
}

Export-ModuleMember -Function TestOnPremToRaw_FS

Export-ModuleMember -Function TestOnPremToRaw_SQL

Export-ModuleMember -Function TestToConformed

Export-ModuleMember -Function TestConformedToSQL

Export-ModuleMember -Function GetRowsCopied

Export-ModuleMember -Function CopyTestFiles

Export-ModuleMember -Function GetInsertedRows

Export-ModuleMember -Function RunPipeline

Export-ModuleMember -Function GetPipelineDateParameter