:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: VARIABLES                                                                    :
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

SETLOCAL
SET PROJECT_NAME=PROJECT_epa_fiscal_impact
SET SUPPORT_LIBRARY = epa_fiscal_impact
SET DOC_ENV_NAME=pmt_docs
SET DL_ENV_NAME=pmt_dl
SET PROC_ENV_NAME=pmt_tools

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: COMMANDS                                                                     :
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Jump to command
GOTO %1

:: Build the local environment from the environment file
:env_dl
    ENDLOCAL & (

        :: Install MAMBA for faster solves
        CALL conda install -c conda-forge mamba -y

        :: Create new environment from environment file
        CALL mamba env create -f environment_download.yml

        :: Activate the enironment so you can get to work
        CALL activate "%DL_ENV_NAME%"

    )
    EXIT /B

:: Build the processing environment
:env_proc
    ENDLOCAL & (

        :: Install MAMBA for faster solves
        CALL conda install -c conda-forge mamba -y

        :: Create new environment from environment file
        CALL mamba env create -f environment_processing.yml

        :: Activate the enironment so you can get to work
        CALL activate "%PROC_ENV_NAME%"

    )
    EXIT /B

:: Build the documentation environment
:env_doc
    ENDLOCAL & (

        :: Install MAMBA for faster solves
        CALL conda install -c conda-forge mamba -y

        :: Create new environment from environment file
        CALL mamba env create -f environment_docs.yml

        :: Activate the enironment so you can get to work
        CALL activate "%DOC_ENV_NAME%"

    )
    EXIT /B

:: Remove the environment
:env_remove
	ENDLOCAL & (
		CALL conda deactivate
		CALL conda env remove --name "%ENV_NAME%" -y
	)
	EXIT /B


