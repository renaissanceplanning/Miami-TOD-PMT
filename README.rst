
Miami-TOD-PMT
=============

Scripts and docs for the Miami-Dade TPO's TOD Performance Monitoring Toolkit

Table of Contents
-----------------


#. `Environment Setup <#environment-setup>`_

   #. `Conda Envs <#building-python-conda-environment>`_

#. ## Environment Setup
   ##### Assumptions


* ArcGIS Pro is installed in a standard location
    ex: C:\Program Files\ArcGIS\Pro
* ArcGIS Pro version > 2.7
* Familiarity with command line interactions

Building python CONDA environment
"""""""""""""""""""""""""""""""""

Env: *pmt_download* [used for download procedures only]
###########################################################


#. Select Windows Start
#. Navigate to 'Python Command Prompt' under ArcGIS folder --> Open
#. In the command window run the below commands:
#. *Create a new environment*
   .. code-block::

       conda create --name %LocalAppData%\ESRI\conda\envs\pmt_download
   4) *Activate the newly created environment*
   .. code-block::

       activate pmt_download
   5) *Install conda packages*
   .. code-block::

       conda install geopandas pandas numpy scipy rtree=0.9.4
       * enter 'y/yes' and return when asked
   6) *Install pip packages*
   .. code-block::

       pip install censusdata osmnx
   ###### Env: *pmt_tools*
   repeat steps 1-3 from above

4) *Clone the existing ArcGIS python environment*

.. code-block::

   ```
   conda create --clone arcgispro-py3 --name %LocalAppData%\ESRI\conda\envs\pmt_tools
   * (this may take some time, so be patient)
   ```

4) *Activate the newly created environment*

.. code-block::

   ```
   activate pmt_tools
   ```

5) *Install conda packages using **conda-forge** channel*

.. code-block::

   ```
   conda install -c conda-forge momepy sphinx dask
   * enter 'y/yes' and return when asked
   * spyder is optional if you want to install a Data Science focused IDE
   ```

6) *Install pip packages*

.. code-block::

   ```
   pip install simpledbf
   ```

*WARNING*
If you have recently updated ArcGIS Pro to a new Major Version, you will need to remove the existing environment and recreate it using
steps 4-7 again.


* Remove env
  .. code-block::

       conda env remove -n pmt_tools

* Follow the above steps to recreate the environment
