from ml_vault import ml_vault 

vault = ml_vault.Vault("jinjin", "test2")
vault2 = ml_vault.Vault("jinjin", "test2") # vault2 = ml_vault.Vault("jinjin2", "test2")

print(vault is vault2)

vault.create_file_list("file_test")
vault.append_file("file_test", "test_location")