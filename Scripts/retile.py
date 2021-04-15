def get_objs(pth):
    return fnmatch.filter(os.listdir(pth),'*.tif')


def get_obj_cat(pth):
    keys_20m = ['B5','B6','B7','B8A','B11','B12']
    obj_lst = get_objs(pth)
    objs_20m = []
    for i in obj_lst:
        for key in keys_20m:
            if i.find(key) != -1:
                objs_20m.append(i)
    objs_10m = [j for j in obj_lst if j not in objs_20m]

    return [objs_10m,objs_20m]


def get_img_stack(pth):
    obj_cat = get_obj_cat(pth)
    stack_10 = []
    stack_20 = []
    profile_10 = None
    profile_20 = None
    # read in 10m objects
    for obj in obj_cat[0]:
        obj_pth = os.path.join(pth,obj)
        img_obj = rasterio.open(obj_pth)
        profile_10 = img_obj.profile
        img_stack = img_obj.read()
        if len(stack_10)==0:
            stack_10 = img_stack
        else:
            stack_10 = np.concatenate((stack_10,img_stack),0)
    # read in 20m objects
    for obj in obj_cat[1]:
        obj_pth = os.path.join(pth,obj)
        img_obj = rasterio.open(obj_pth)
        profile_20 = img_obj.profile
        img_stack = img_obj.read()
        if len(stack_20)==0:
            stack_20 = img_stack
        else:
            stack_20 = np.concatenate((stack_20,img_stack),0)
    return stack_10,stack_20,profile_10,profile_20


def gen_weights(img_stack,position,buf):
    # position 1: concat on 6 o'clock; 2: concat on 3 o'clock
    row = np.shape(img_stack)[1]
    col = np.shape(img_stack)[2]
    # cal weights
    w_2 = np.cumsum(np.ones((1, buf))) * (1 / buf)
    w_1 = 1 - w_2
    if position == 1:
        # padding first, then do the transpose
        w_pad = np.ones((1, row - buf))
        w_1 = np.hstack((w_pad, w_1.reshape(1, -1)))
        w_1 = np.hstack((w_1.reshape(1, -1), w_pad))
        w_1 = w_1.repeat([col, ], axis=0)
        w_2 = np.hstack((w_pad, w_2.reshape(1, -1)))
        w_2 = np.hstack((w_2.reshape(1, -1), w_pad))
        w_2 = w_2.repeat([col, ], axis=0)
        # transpose
        w_1 = w_1.T
        w_2 = w_2.T
    elif position == 2:
        # add paddings
        w_pad = np.ones((1, col - buf))
        w_1 = np.hstack((w_pad, w_1.reshape(1, -1)))
        w_1 = np.hstack((w_1.reshape(1, -1), w_pad))
        w_1 = w_1.repeat([row, ], axis=0)
        w_2 = np.hstack((w_pad, w_2.reshape(1, -1)))
        w_2 = np.hstack((w_2.reshape(1, -1), w_pad))
        w_2 = w_2.repeat([row, ], axis=0)


    return w_1,w_2


def merge(stackA,stackB,position,buf):
    # position 1: concat on 6 o'clock; 2: concat on 3 o'clock
    if np.shape(stackA) != np.shape(stackB):
        # not available for non-equal band stack
        if np.shape(stackA)[0] != np.shape(stackB)[0]:
            raise Exception("######The bands of inputs do equal with each other!######")
        else:
            bands = np.shape(stackA)[0]
            row_A = np.shape(stackA)[1]
            row_B = np.shape(stackB)[1]
            col_A = np.shape(stackA)[2]
            col_B = np.shape(stackB)[2]
            row_max = max(row_A, row_B)
            col_max = max(col_A, col_B)

            if position == 1:
                # pad the right first
                # then pad the bottom
                if col_A >= col_B:
                  # for stackA
                  ## no need to pad the right
                  ## pad the bottom
                  img_patchBOT = np.zeros(row_B-buf,col_A)
                  # for stackB
                  ## pad the right
                  ## pad the top
                  img_patchRGT = np.zeros(row_B,col_A-col_B)
                  img_patchTOP = np.zeros(row_A-buf,col_A)

                  for i in range(bands):
                    stackA[i] = np.vstack((stackA[i],img_patchBOT))
                    stackB[i] = np.hstack((stackB[i],img_patchRGT))
                    stackB[i] = np.vstack((img_patchTOP,stackB[i]))
                else:
                  # for stackA
                  ## pad the right
                  ## pad the bottom
                  img_patchRGT = np.zeros(row_A,col_B-col_A)
                  img_patchBOT = np.zeros(row_B-buf,col_B)
                  # for stackB
                  ## no need to pad the right
                  ## pad the top
                  img_patchTOP = np.zeros(row_A-buf,col_B)

                  for i in range(bands):
                    stackA[i] = np.hstack((stackA[i], img_patchRGT))
                    stackA[i] = np.vstack((stackA[i],img_patchBOT))
                    stackB[i] = np.vstack((img_patchTOP,stackB[i]))
                  

            elif position == 2:
              # 



    else:
        bands = np.shape(stackA)[0]
        row = np.shape(stackA)[1]
        col = np.shape(stackA)[2]
        result = []
        # prepare weights
        w_a, w_b = gen_weights(stackA, position, buf)

        # merge imgs
        if position == 1:
            # attach zero patch to img to fit the size of merged
            img_patch = np.zeros((row-buf, col))
            for i in range(bands):
                tmp_a = np.vstack((stackA[i], img_patch))
                tmp_b = np.vstack((img_patch, stackB[i]))
                merged = tmp_a*w_a + tmp_b*w_b
                merged = merged.astype(rasterio.int16)
                if len(result) == 0:
                    result = merged
                else:
                    result = np.concatenate((result, merged), axis=0)
            # reshape it to multi band format
            result = np.reshape(result, (bands, 2 * row - buf, col))
        elif position == 2:
            img_patch = np.zeros((row, col-buf))
            for i in range(bands):
                tmp_a = np.hstack((stackA[i], img_patch))
                tmp_b = np.hstack((img_patch, stackB[i]))
                merged = tmp_a * w_a + tmp_b * w_b
                if len(result) == 0:
                    result = merged
                else:
                    result = np.concatenate((result, merged), axis=0)
            # reshape
            result = result.astype(rasterio.int16)
            result = np.reshape(result,(bands, row, 2*col-buf))

    return result








obj1_pth= r'F:\Clark\2020_Spring_Independent_Study\Sentinel2_SampleData\SENTINEL2X_20201216-000000-000_L3A_T30NXP_C_V1-2'
obj2_pth= r'F:\Clark\2020_Spring_Independent_Study\Sentinel2_SampleData\SENTINEL2X_20201216-000000-000_L3A_T30NYP_C_V1-2'
obj3_pth= r'F:\Clark\2020_Spring_Independent_Study\Sentinel2_SampleData\SENTINEL2X_20201216-000000-000_L3A_T30NXN_C_V1-2'
obj4_pth= r'F:\Clark\2020_Spring_Independent_Study\Sentinel2_SampleData\SENTINEL2X_20201216-000000-000_L3A_T30NYN_C_V1-2'

# 1&2
tmp_a10, tmp_a20, prof_a10, prof_a20 = get_img_stack(obj1_pth)
tmp_b10, tmp_b20, prof_b10, prof_b20 = get_img_stack(obj2_pth)

result12_10 = merge(tmp_a10,tmp_b10,2,984)
result12_20 = merge(tmp_a20,tmp_b20,2,492)
prof_a10.update(width=result12_10.shape[2], count=result12_10.shape[0])
prof_a20.update(width=result12_20.shape[2], count=result12_20.shape[0])
with rasterio.open('retile/result12_10.tif', 'w', **prof_a10) as dst:
    dst.write(result12_10)
    dst.close()
with rasterio.open('retile/result12_20.tif', 'w', **prof_a20) as dst:
    dst.write(result12_20)
    dst.close()

# 3&4
tmp_c10, tmp_c20, prof_c10, prof_c20 = get_img_stack(obj3_pth)
tmp_d10, tmp_d20, prof_d10, prof_d20 = get_img_stack(obj4_pth)

result34_10 = merge(tmp_c10,tmp_d10,2,984)
result34_20 = merge(tmp_c20,tmp_d20,2,492)
prof_c10.update(width=result34_10.shape[2], count=result34_10.shape[0])
prof_c20.update(width=result34_20.shape[2], count=result34_20.shape[0])
with rasterio.open('retile/result34_10.tif', 'w', **prof_c10) as dst:
    dst.write(result34_10)
    dst.close()
with rasterio.open('retile/result34_20.tif', 'w', **prof_c20) as dst:
    dst.write(result34_20)
    dst.close()

# release mid product
gc.collect()

result12_10 = rasterio.open('retile/result12_10.tif')
result12_10 = result12_10.read()
result12_20 = rasterio.open('retile/result12_20.tif')
result12_20 = result12_20.read()

result34_10 = rasterio.open('retile/result34_10.tif')
result34_10 = result34_10.read()
result34_20 = rasterio.open('retile/result34_20.tif')
result34_20 = result34_20.read()


result_10 = merge(result12_10,result34_10,1,984)
result_20 = merge(result12_20,result34_20,1,492)
profile_10 = rasterio.open('retile/result12_10.tif').profile
profile_10.update(count=result_10.shape[0],
                  height=result_10.shape[1],
                  width=result_10.shape[2])
profile_20 = rasterio.open('retile/result12_20.tif').profile
profile_20.update(count=result_20.shape[0],
                  height=result_20.shape[1],
                  width=result_20.shape[2])

with rasterio.open('retile/result_10.tif', 'w', **profile_10) as dst:
    dst.write(result_10)
    dst.close()
with rasterio.open('retile/result_20.tif', 'w', **profile_20) as dst:
    dst.write(result_20)
    dst.close()




# tmp = get_obj_cat(obj4_pth)
# tmp_10, tmp_20 = get_img_stack(obj2_pth)
# stackB = tmp_10
# buf = 984

tmp_img1 = rasterio.open(os.path.join(obj1_pth,tmp[0][0]))
# tmp_img1 = tmp_img1.read()
tmp_img2 = rasterio.open(os.path.join(obj4_pth,tmp[0][0]))
# tmp_img2 = tmp_img2.read()
tmp_img1 = np.ones((1,3,4))
tmp_img2 = np.ones((1,3,4))
tmp_img3 = np.ones((2,3,4))
tmp_patch = np.zeros((3,4))
tmp_img1 = np.hstack((tmp_img1.reshape(3,4),tmp_patch))
tmp_img2 = np.hstack((tmp_patch,tmp_img2.reshape(3,4)))
# w_1 = np.cumsum(np.ones((1,2)))*2
# w_1 = np.hstack((np.ones((1,2)),w_1.reshape(1,2)))
# w_1 = np.hstack((w_1,np.ones((1,2))))
# w_1 = w_1.repeat([3,],axis=0)
tmp_w = np.cumsum(np.ones((1,2)))*2
tmp_w = np.hstack((np.ones((1,2)),tmp_w.reshape(1,2)))
tmp_w = np.hstack((tmp_w,np.ones((1,2))))
tmp_w = tmp_w.repeat([3,],axis=0)

np.concatenate((tmp_a,tmp_b),1)
np.shape(np.concatenate((tmp_img1,tmp_img2),1))












