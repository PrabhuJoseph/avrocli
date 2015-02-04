package avrocli.avro.configuration;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.AbstractHierarchicalFileConfiguration;
import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.DefaultConfigurationBuilder;

public final class Configuration {

	private static final String CONFIG_FILE = "/config.xml";

	private CombinedConfiguration combinedConfiguration;
	private static Configuration configInstance;

	private Configuration() {

		DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
		URL configFileURL = getClass().getResource(CONFIG_FILE);
		builder.setFile(new File(configFileURL.getFile()));

		try {
			combinedConfiguration = builder.getConfiguration(true);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}

	}

	public static Configuration getInstance() {

		if (configInstance == null) {

			synchronized (Configuration.class) {
				if (configInstance == null) {
					configInstance = new Configuration();
				}
			}
		}

		return configInstance;
	}

	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}

	public String getProperty(String key) {
		if (!combinedConfiguration.containsKey(key)) {
			throw new NoSuchElementException(key);
		}
		return combinedConfiguration.getString(key);
	}

	public String getProperty(String key, String defaultValue) {
		return combinedConfiguration.getString(key, defaultValue);
	}

	public boolean isEmpty() {
		return combinedConfiguration.isEmpty();
	}

	public void setProperty(String key, String value) {

		if (!combinedConfiguration.containsKey(key))
			throw new NoSuchElementException(key);
		combinedConfiguration.setProperty(key, value);
		try {
			((AbstractHierarchicalFileConfiguration) (combinedConfiguration.getConfiguration(0))).save();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public List<String> getKeysAsList() {
		List<String> resultList = new ArrayList<String>();
		Iterator<String> keyIterator = (Iterator<String>) combinedConfiguration.getKeys();
		while (keyIterator.hasNext()) {
			resultList.add(keyIterator.next());
		}
		return resultList;
	}

	public boolean isExists(String key) {
		return combinedConfiguration.containsKey(key);
	}

}
