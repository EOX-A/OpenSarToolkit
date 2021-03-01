ARG BASE_CONTAINER=jupyter/scipy-notebook:016833b15ceb
FROM $BASE_CONTAINER

USER root

ARG OST_BRANCH

ENV HOME=/home/$NB_USER
ENV OTB_VERSION="7.1.0" \
    TBX_VERSION="8" \
    TBX_SUBVERSION="0"
ENV TBX="esa-snap_sentinel_unix_${TBX_VERSION}_${TBX_SUBVERSION}.sh" \
  SNAP_URL="https://download.esa.int/step/snap/${TBX_VERSION}.${TBX_SUBVERSION}/installers/" \
  OTB=OTB-${OTB_VERSION}-Linux64.run \
  HOME=$HOME \
  PATH=$PATH:$HOME/programs/snap/bin:$HOME/programs/OTB-${OTB_VERSION}-Linux64/bin

RUN sed -i -e 's:(groups):(groups 2>/dev/null):' /etc/bash.bashrc

# install gdal as root
RUN apt-get update && \
    apt-get install -yq build-essential gcc g++ git python3 libgfortran5 && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip && \
    alias python=python3

# Grant access to folders for user
RUN fix-permissions $HOME && \
    mkdir $HOME/programs && \
    fix-permissions $HOME/programs

# copy the snap installation config file into the container
COPY snap.varfile $HOME/programs/

# Download and install SNAP
RUN cd  $HOME/programs && \
    wget -nv $SNAP_URL/$TBX && \
    chmod +x $TBX && \
    ./$TBX -q -varfile snap.varfile && \
    rm $TBX && \
    rm snap.varfile

# set usable memory to 12G
RUN echo "-Xmx12G" > /home/ost/programs/snap/bin/gpt.vmoptions

#  Download and install ORFEO Toolbox
#RUN cd $HOME/programs && \
#    wget -nv https://www.orfeo-toolbox.org/packages/${OTB} && \
#    chmod +x $OTB && \
#    ./${OTB} && \
#    rm -f OTB-${OTB_VERSION}-Linux64.run

USER $NB_UID

RUN conda install --quiet --yes --force-reinstall --update-all \
    oauthlib \
    gdal \
    fiona \
    rasterio \
    shapely \
    xarray \
    zarr \
    psycopg2 \
    geopandas \
    cartopy \
    tqdm \
    lightgbm \
    descartes && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR

# jupyter geojson as regular user
RUN jupyter labextension install @jupyterlab/geojson-extension

# get OST and tutorials
RUN cd $HOME && \
    git clone https://github.com/EOX-A/OpenSarToolkit.git && \
    cd $HOME/OpenSarToolkit && \
    git checkout -b $OST_BRANCH && \
    pip install setuptools && \
    pip install -r requirements.txt && \
    pip install -r requirements_test.txt && \
    python setup.py install
#    cd $HOME && \
#    git clone https://github.com/EOX-A/OST_Notebooks.git

# Return Home
RUN cd $HOME

# Make jupyter lab availible at this docker image
EXPOSE 8888
CMD jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root