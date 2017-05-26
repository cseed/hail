#!/bin/bash

gcloud -q --project seqr-project container clusters delete --async --zone us-central1-a seqr-backend
