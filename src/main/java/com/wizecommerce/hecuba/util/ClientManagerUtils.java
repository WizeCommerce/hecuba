package com.wizecommerce.hecuba.util;

import java.util.Date;

import com.wizecommerce.hecuba.HecubaConstants;

public class ClientManagerUtils {
	private static final ClientManagerUtils instance = new ClientManagerUtils();

	private ClientManagerUtils() {
	}

	public static ClientManagerUtils getInstance() {
		return instance;
	}

	public String convertValueForStorage(Object input) {
		if (input == null) {
			return "null";
		}

		if (input instanceof Date) {
			return HecubaConstants.DATE_FORMATTER.print(((Date) input).getTime());
		}

		return input.toString();
	}
}
