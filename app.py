#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.cdk_stack import ImageUploadAndProcessingStack


app = cdk.App()
ImageUploadAndProcessingStack(app, "ImageUploadAndProcessingStack") 
app.synth()

