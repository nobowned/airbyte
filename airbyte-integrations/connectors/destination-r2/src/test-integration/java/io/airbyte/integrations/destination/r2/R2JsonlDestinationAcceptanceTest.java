/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.r2;

import io.airbyte.cdk.integrations.destination.s3.S3BaseJsonlDestinationAcceptanceTest;
import io.airbyte.cdk.integrations.destination.s3.StorageProvider;

public class R2JsonlDestinationAcceptanceTest extends S3BaseJsonlDestinationAcceptanceTest {

  @Override
  protected String getImageName() {
    return "airbyte/destination-r2:dev";
  }

  @Override
  public StorageProvider storageProvider() {
    return StorageProvider.CF_R2;
  }

}
