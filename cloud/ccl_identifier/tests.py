
	'''
	# Testing FACEBOOK - MARKETPLACE_BASED
	#relevant_ocrs_on_observation_frame_test("b6ea440c-8bd5-4333-839e-e52e9040531e/temp/1755756819000.929badbe-d7a1-4f76-9aef-4ed04a36df0e") # general [PASSED]
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755697067250.30e1e38f-24ec-4194-9d96-4dc1f0fae6db") # general [PASSED]
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755697071711.1df1c292-234f-459c-935f-d30453091832") # cropped - should be nonsense [PASSED]
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755697062250.39b7466c-2d80-473a-bca7-6dd2d7193f39") # ellipsis [PASSED]
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755696941497.8416fc8e-b53c-4bb6-8127-5d96a3b20293") # emoji [PASSED]
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755694305514.df16249d-fe77-43dc-879e-336164b745bc") # cropped and irregular sponsored term - should return nothing [PASSED]
	relevant_ocrs_on_observation_frame_test("21f5c684-4050-4a86-bb3e-d26959a3eb19/temp/1755677688392.be55d45a-a2e2-46e3-806f-7ff514728222") # dark on white [PASSED]
	relevant_ocrs_on_observation_frame_test("21f5c684-4050-4a86-bb3e-d26959a3eb19/temp/1755677672831.03b55a8d-ddef-415a-b6bc-e7180a4dfa13") # dark on white [PASSED]
	

	
	# Testing FACEBOOK - FEED_BASED
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1756373711923.bed1e67e-0162-4974-87b6-1f960ea9a8d5") # general [PASSED]
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756353783245.752af6e8-93f2-4ec6-ab00-01e8f4ea08c9") # disclaimer on sponsored [PASSED]
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756353908932.5d89a9dc-92b7-4615-94cd-3de8a901c0b5") # verified tick in name [PASSED]
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756353443491.fdb12586-7a5c-40f7-b5b8-37192be61954") # dots in name [PASSED]
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756353420885.6aab7e9b-5477-4eb3-9ef9-98f1e15a0dd5") # lq [PASSED]
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756353572875.061a6339-3e40-4783-816c-4b4e9c18718b") # NOT AN AD [PASSED]
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755352517000.05e7679c-32e1-4f30-bd4c-44185579bf1f") # music feature
	relevant_ocrs_on_observation_frame_test("af04a599-1f20-4340-8b1c-01decaf7c17e/temp/1755351382500.a299aeae-dda6-40f7-8c72-caa8ff66fb9d") # dark on white
	

	
	# Testing FACEBOOK - REEL_BASED
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1755937169133.249ceb03-25d9-4e48-9d7e-5a213c184eed") # misclassified  
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755903398923.f91c7f93-c0dc-4c1b-9472-4eb2068a263a") # general non-button
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755903436924.125ef278-4cfb-4dd1-a59c-ab21cbd492be")# general button
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755903303370.e055b3be-05b4-41e6-a0d6-05b3b8464934") # carousel underneath
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755902963696.23d38764-2d2f-474d-9288-9334a73b634c") # malformed
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755902255000.d025daa2-9620-4218-bd8c-e3f3c0615288") # friend feature
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755901500774.baf7d74d-e5f2-4275-af45-c23897e8d2e2") # footer ad composite
	relevant_ocrs_on_observation_frame_test("d381c5bb-c31d-484d-bb8f-766cdf818fa3/temp/1755895605922.fff64871-95f9-46ab-ac14-4c2387adcc93") # not an ad
	
	
	# Testing FACEBOOK - REEL_FOOTER_BASED
	relevant_ocrs_on_observation_frame_test("57f6e9f1-f0a8-472a-aef6-a10fb6ab3ea8/temp/1755759132500.b32772e6-8678-4b3c-bc18-f4b5f50f1fd1") # adjacent and dotted
	relevant_ocrs_on_observation_frame_test("b6ea440c-8bd5-4333-839e-e52e9040531e/temp/1755754645250.3b303a98-7853-4fcd-a41f-6284fd5e09cd") # tick of verification
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755732102243.acc6b7a8-6c46-4656-81c1-203bf36026db") # text above
	relevant_ocrs_on_observation_frame_test("57f6e9f1-f0a8-472a-aef6-a10fb6ab3ea8/temp/1755728691389.6b071637-b6d0-4d5c-aff3-2fb5e85aa97a") # poorly captured
	relevant_ocrs_on_observation_frame_test("57f6e9f1-f0a8-472a-aef6-a10fb6ab3ea8/temp/1755475810270.6375ac48-8357-470e-886a-055f981e6248") # really small
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755432599084.2185d441-f3db-48e2-b142-1d24319ed1fa") # general
	relevant_ocrs_on_observation_frame_test("b6ea440c-8bd5-4333-839e-e52e9040531e/temp/1755412556574.10a666ec-f1a4-43c8-b897-82c0ea9b49a1") # cutoff
	

	
	# Testing FACEBOOK - STORY_BASED
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755259972000.f58eb936-bf02-4fc1-849d-4ee2356fbfac") # general
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755261835472.714aac31-572c-438d-947b-40c373212429") # misclassified - actually feed-bsaed
	relevant_ocrs_on_observation_frame_test("a8212f89-7b16-4a7c-8958-d1552fac3bcc/temp/1755239161377.455462d4-e228-43bc-bbaf-019f888665b3") # general
	relevant_ocrs_on_observation_frame_test("a8212f89-7b16-4a7c-8958-d1552fac3bcc/temp/1755239121500.3b67dfcf-a648-4feb-bee4-b00a16b2ce04") # general
	relevant_ocrs_on_observation_frame_test("a8212f89-7b16-4a7c-8958-d1552fac3bcc/temp/1755239102948.3ce8b7d6-be19-46af-bdba-3934fcec24d2") # text adjacent
	relevant_ocrs_on_observation_frame_test("dd4d5d38-96a7-4095-ba1d-80e0aaaa3b14/temp/1755177235165.7cf92879-c4ba-48a2-bf7f-cd0ca6d6a490") # low contrast
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755164264807.241696e3-9a3f-49c6-a432-fab950e1faa4") # low contrast
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755164133842.46437105-e500-438f-9e7b-7ce2995d74ba") # low contrast
	ipdb.set_trace()
	print(result["text"])
	
	
	# Testing INSTAGRAM - REEL_BASED
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756385876836.140355a3-13b5-4790-8e79-ce0125322de5") # intermediate texts
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756385853989.d3d24a55-7524-4f9e-91c4-7f4ec2f0bd4a")
	relevant_ocrs_on_observation_frame_test("88a7dbf3-feb9-4fec-b0b5-6341526157f7/temp/1756379501411.5ac660a9-3ec6-4f6f-bfc3-8171359e8b0b")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756363818248.a0ec6481-82ed-464e-b675-7253542c9b4d")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756263010742.1e9381a4-6114-465b-bdd2-f19a1b4ef98c")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756262238320.91e43496-e0f5-4361-9a3e-002bfc8a9e5e")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756260537246.2413590d-71ce-4bb0-88bb-8c23b7c257ad") # extends beyond general x threshold
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756259322444.ea368205-1b17-48ba-8c66-88f2324cd83f")
	

	
	# Testing INSTAGRAM - STORY_BASED
	relevant_ocrs_on_observation_frame_test("41a3ac10-d88f-42d1-918d-233b980f5c4c/temp/1755617944884.e8cc12f2-396f-4459-9a96-00988f036ad9")
	relevant_ocrs_on_observation_frame_test("3e3685ee-c706-45e7-bfb9-43fcdc68ae61/temp/1755660741728.41e52fd3-cba4-4014-8647-ac434b07bb24")
	relevant_ocrs_on_observation_frame_test("3e3685ee-c706-45e7-bfb9-43fcdc68ae61/temp/1755660759719.96c2fcd7-ef8e-4638-9b2d-dff95b95ec73") # lq
	relevant_ocrs_on_observation_frame_test("3e3685ee-c706-45e7-bfb9-43fcdc68ae61/temp/1755660846480.179543ce-685e-4306-bff8-354dac899170") # underlying text
	relevant_ocrs_on_observation_frame_test("3e3685ee-c706-45e7-bfb9-43fcdc68ae61/temp/1755660922725.6c2ff5cf-4247-4055-b5d9-bfe9fe273ce8") # no title
	relevant_ocrs_on_observation_frame_test("3e3685ee-c706-45e7-bfb9-43fcdc68ae61/temp/1755661024604.e4749a91-9530-42ec-8813-018c0211d034") # slanted
	relevant_ocrs_on_observation_frame_test("3e3685ee-c706-45e7-bfb9-43fcdc68ae61/temp/1755661057407.d7d65599-163c-43b6-8ce4-967311644410") # 
	relevant_ocrs_on_observation_frame_test("3ac76130-eea8-42d7-bb38-a38ceb3514a5/temp/1755689963000.c2cd6d29-f3f1-49c1-9380-344da8c3bb34") # slanted
	relevant_ocrs_on_observation_frame_test("41a3ac10-d88f-42d1-918d-233b980f5c4c/temp/1755606413639.0ca1e66e-ae29-406e-bf84-b1b334e846a1")
	

	# Testing INSTAGRAM - FEED_BASED
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756505882461.dc111a5e-53d1-4e3f-8151-a2e92714c3bc")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756505623666.2cb7c56a-9b75-47e6-9a02-22a895a03c37")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756428528244.501d455b-9b72-4001-95c6-6876521e24db")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756428307984.ea922a05-2635-475a-83ea-7a92e5628927")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756428384000.021bf77f-e0f1-4617-8c85-d5c3ecc2a47a")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756364105968.c80f527c-05c9-42c4-9c7e-4aa5986f6d75")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756363694182.d0303a88-10e1-4dce-8e91-4f683bf2668c")
	relevant_ocrs_on_observation_frame_test("567163af-df7a-4824-a2ab-26d9cae566ab/temp/1756302921239.7e729ef4-bbbd-4e22-93eb-501e1970a201")

	'''
	'''
	# Testing TIKTOK - THUMBNAIL
	relevant_ocrs_on_observation_frame_test("af04a599-1f20-4340-8b1c-01decaf7c17e/temp/1755311209466.231dc728-1c17-4206-96e5-e7c625270b4d")
	relevant_ocrs_on_observation_frame_test("af04a599-1f20-4340-8b1c-01decaf7c17e/temp/1755311202862.be814064-8bc6-4574-b2b5-f6794fa2b9e6")
	relevant_ocrs_on_observation_frame_test("ae158b1b-78df-4a18-94cd-6e302d3144be/temp/1754818775750.50400bfe-f9e9-4d11-a1dd-3a9abf46aa81")
	relevant_ocrs_on_observation_frame_test("9297258f-5123-44f9-be20-858379b3542d/temp/1754988853938.76c5d4fe-e1d1-4376-a7bf-7c16e555fa64")
	relevant_ocrs_on_observation_frame_test("e46a8a4c-04ae-4ee7-8cd3-ce46f422c4d5/temp/1755066283685.5660b8d0-3d92-46d7-b591-d9edf85ec1d9")
	relevant_ocrs_on_observation_frame_test("c7e2e6b7-0a4d-4cfd-8ddf-3ab76b879f9f/temp/1754666487436.356bf29e-d8f4-431a-80ae-544dc140311c")
	relevant_ocrs_on_observation_frame_test("9297258f-5123-44f9-be20-858379b3542d/temp/1754549027834.0884fd2b-10f4-4415-a64e-4eb9ffeaf21d")
	relevant_ocrs_on_observation_frame_test("9297258f-5123-44f9-be20-858379b3542d/temp/1754473539833.b648dd23-faf0-4850-b236-dd0167026a6f")
	'''
	#ipdb.set_trace()

	'''
	# Testing TIKTOK - REEL_FROM_SEARCH
	relevant_ocrs_on_observation_frame_test("af04a599-1f20-4340-8b1c-01decaf7c17e/temp/1755682401884.ff405c2c-cc6c-425f-94e4-e5932e2751f0")
	relevant_ocrs_on_observation_frame_test("af04a599-1f20-4340-8b1c-01decaf7c17e/temp/1755682368296.caa58aab-9f79-433e-9b76-c7c66cebb11c")
	relevant_ocrs_on_observation_frame_test("c9f003c1-3986-4df6-95cb-11d1c6c07001/temp/1755639898683.a5d25941-4ebd-4b09-a6b6-e8bf4d364a4c")
	relevant_ocrs_on_observation_frame_test("af04a599-1f20-4340-8b1c-01decaf7c17e/temp/1755612409218.14429794-e2a4-45d8-915c-e07543f5c213")
	relevant_ocrs_on_observation_frame_test("e46a8a4c-04ae-4ee7-8cd3-ce46f422c4d5/temp/1755091510986.98df23c3-28ad-41d4-8a35-bd32ac861c71")
	relevant_ocrs_on_observation_frame_test("5fa73d5e-c66e-4ffd-806d-4c0889460ad4/temp/1755074409536.c5a30c04-d95b-4e3c-bfe5-69cd26b86244")
	relevant_ocrs_on_observation_frame_test("5fa73d5e-c66e-4ffd-806d-4c0889460ad4/temp/1755074625220.91966133-bab1-471c-a87c-d41992e635c4")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755085837320.9ae47e22-8966-4685-b82c-35fee8bf2331")
	'''

	'''
	# Testing TIKTOK - REEL_FROM_HOME
	relevant_ocrs_on_observation_frame_test("c9f003c1-3986-4df6-95cb-11d1c6c07001/temp/1755264181247.c84d1cbf-d430-46ba-b2aa-2e45e44d1088")
	relevant_ocrs_on_observation_frame_test("6fa5067a-5a1d-40f6-9e15-7f84ff7e8301/temp/1755259422172.2032e9a6-29d2-4be1-92e1-4595a416dd71") # malformed
	relevant_ocrs_on_observation_frame_test("6fa5067a-5a1d-40f6-9e15-7f84ff7e8301/temp/1755232009702.84e99f3a-6050-474f-a647-a916912ccfbd") # malformed
	relevant_ocrs_on_observation_frame_test("6fa5067a-5a1d-40f6-9e15-7f84ff7e8301/temp/1755205093476.07e6a6c7-6616-4ff3-aed2-548b3c183c3a")
	relevant_ocrs_on_observation_frame_test("6fa5067a-5a1d-40f6-9e15-7f84ff7e8301/temp/1755201394380.5ed58518-7cab-4bba-924e-45040be40878")
	relevant_ocrs_on_observation_frame_test("e46a8a4c-04ae-4ee7-8cd3-ce46f422c4d5/temp/1755003405778.27f1346a-aa7b-4201-907f-de6d39ae9638")
	relevant_ocrs_on_observation_frame_test("e46a8a4c-04ae-4ee7-8cd3-ce46f422c4d5/temp/1754923522742.144520a3-0f70-4c25-98c1-0c79d8da9f36")
	relevant_ocrs_on_observation_frame_test("e46a8a4c-04ae-4ee7-8cd3-ce46f422c4d5/temp/1754488955709.0023d9d0-94ec-4d6a-9693-68b8821d350f")
	'''

	'''
	# Testing YOUTUBE - PREVIEW_PORTRAIT_BASED
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755947046223.ed6bdd94-93b2-402f-87b1-60fe7c3fa3b0")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755946742267.e6b6b6fa-54f0-4dcd-b3d2-e3547bc99cc0")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755934408014.6377d747-4926-40d3-a3be-9321e569a6be")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755937797439.8716e942-191e-46ce-bdba-e8579fc12635")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755938163431.2fe500da-29f4-4d44-a4f5-36c24b9145f2")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755939773019.7d16bb02-bd1f-4c43-a82b-02edc8bd968b")
	relevant_ocrs_on_observation_frame_test("57f6e9f1-f0a8-472a-aef6-a10fb6ab3ea8/temp/1754908818128.99aebc5b-3c2d-4a60-bccc-286c6580b194")
	relevant_ocrs_on_observation_frame_test("6bcea4cc-e6f6-4483-97f9-4f07e4ab85e2/temp/1754913759828.05cc7c3e-e1ff-4a54-8fab-2600120d3a0e")
	'''

	'''
	# Testing YOUTUBE - REEL_BASED
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755817172713.8205d203-20d5-4749-a6e3-10bdb7116cf0")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755817245000.9a75d1f3-abfb-41be-ab5f-98330e6243df")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755817186937.12fdf556-6022-43c1-ae0f-2cc785497507")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755817092683.6b017acb-167a-4320-8dac-d947736025f8")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755815900883.eab02551-1c3a-418b-a49a-059aac4d4939")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755815766000.12dcfbc2-cebc-43a1-8370-8f15a5c4404f")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755815985029.605d4629-1e1d-4c6b-9447-2c0c49d6e0d4")
	relevant_ocrs_on_observation_frame_test("08155cc8-4cfe-49e4-b6f3-301125673899/temp/1755244323250.4d3989b9-c95d-463d-8adb-c0369ff14d84")
	relevant_ocrs_on_observation_frame_test("08155cc8-4cfe-49e4-b6f3-301125673899/temp/1755244228000.5081a308-30e0-4b39-9770-bf70fca6d750")
	'''

	'''
	# Testing YOUTUBE - GENERAL_FEED_BASED
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755939340690.4e740712-dddd-4d39-9e23-51843862a717")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755946511842.ab245211-fe00-4997-88c5-3708e5440dab")
	relevant_ocrs_on_observation_frame_test("98c2d38a-1ce5-41d1-b50e-a3dcc45295b8/temp/1755946880589.1a6318d9-7969-4132-b5f2-01d29534ca17")
	relevant_ocrs_on_observation_frame_test("eced5c74-871b-457e-9eb2-708bc4aed581/temp/1755897887196.c2c484d2-ae44-4787-ad3f-f27daf3c5a0c")
	relevant_ocrs_on_observation_frame_test("57f6e9f1-f0a8-472a-aef6-a10fb6ab3ea8/temp/1755731727600.bb104846-bada-40f5-a201-e62e1488bdd5")
	relevant_ocrs_on_observation_frame_test("08155cc8-4cfe-49e4-b6f3-301125673899/temp/1755728709341.06ab7dbf-92fe-4bfc-b56d-b844fec707bc")
	relevant_ocrs_on_observation_frame_test("57f6e9f1-f0a8-472a-aef6-a10fb6ab3ea8/temp/1755676429000.3c14bed8-f4e8-4593-ad68-1e3fd0ce9f6e")
	relevant_ocrs_on_observation_frame_test("08155cc8-4cfe-49e4-b6f3-301125673899/temp/1755675973294.6a461ea3-fc27-4bbc-97f2-46363840748d")
	relevant_ocrs_on_observation_frame_test("21f5c684-4050-4a86-bb3e-d26959a3eb19/temp/1755675518078.696f5195-9060-46cf-8125-7e425796e788")
	'''