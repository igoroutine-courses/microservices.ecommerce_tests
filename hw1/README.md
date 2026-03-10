# E-Commerce Platform

(основано на реальных событиях)

## Предыстория

Вы работаете в большом маркетплейсе `Igoroutine Shop`. За хорошую работу вас повысили и расширили зону ответственности, теперь в вашу
юрисдикцию входит платформа электронной коммерции.

![img.png](./docs/img/img.png)

Вы собрали функциональные требования и спроектировали два сервиса:

- **Cart** — управление корзиной пользователя
- **Logistics and Order Management System (LOMS)** — логистика и управление заказами


Изначально пользователь добавляет товары в корзину:

![img_1.png](./docs/img/img_1.png)

Важно, что при добавлении товара проверяются его существование и наличие на складе:

![img_2.png](./docs/img/img_2.png)

Далее пользователь переходит к оплате:

![img_3.png](./docs/img/img_3.png)

Заказ можно оплатить или отменить:

![img_4.png](./docs/img/img_4.png)

Оплата:
![img_5.png](./docs/img/img_5.png)

Отмена:
![img_6.png](./docs/img/img_6.png)


## Описание

Как грамотный архитектор, вы решили начать с [UML-диаграмм](https://ru.wikipedia.org/wiki/Диаграмма_(UML)) для основных сценариев.

Добавление товара в корзину:

![cart-item-add.png](./docs/img/cart-item-add.png)

Просмотр корзины

![cart-list.png](./docs/img/cart-list.png)

Создание заказа

cart:

![cart-checkout.png](./docs/img/cart-checkout.png)

loms:

![loms-order-create.png](./docs/img/loms-order-create.png)

Оплата заказа:

![loms-order-pay.png](./docs/img/loms-order-pay.png)

Отмена заказа:

![loms-order-cancel.png](./docs/img/loms-order-cancel.png)

Вам необходимо разработать сервисы [loms](./loms) и [cart](./cart), реализовав и протестировав вышеперечисленные сценарии.

## Задание

Для вашего удобства в шаблоне репозитория лежат заготовки с кодом. Вам даны .proto файлы для сервисов cart и loms.

- [cart.proto](./cart/api/v1/cart.proto) — контракт для работы с корзиной
- [product.proto](./loms/api/product/v1/product.proto) — контракт для работы с продуктами (проверка существования продукта, добавление)
- [stocks.proto](./loms/api/stocks/v1/stocks.proto)  — контракт для получения информации об остатках и управления резервами товаров
- [loms.proto](./loms/api/loms/v1/loms.proto)  — контракт для работы с заказами

> proto контракты можно дополнять, не ломая обратную совместимость

## Пример реализации

Рассмотрим сценарий добавления товара в корзину пользователя.

![cart-item-add.png](./docs/img/cart-item-add.png)

Изначально создадим клиента из Cart в ProductService (loms):

```go
lomsConn, err := grpc.NewClient(
    cfg.Clients.LOMSGrpcAddr,
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)

if err != nil {
    logger.Fatal("failed to connect to server", zap.Error(err))
}

defer func(conn *grpc.ClientConn) {
    _ = conn.Close()
}(lomsConn)

productClient := productadapter.NewProductClient(
    product.NewProductServiceClient(lomsConn),
)
```

Далее напишем хендлер для добавления товара в cart:

```go
func (s *cartServer) AddItem(ctx context.Context, req *cartpb.AddItemRequest) (*emptypb.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if err := s.itemService.AddItem(ctx, req.UserId, req.Sku, req.Count); err != nil {
		switch {
		case errors.Is(err, entity.ErrProductNotFound):
			return nil, status.Errorf(codes.NotFound, "product not found")
		case errors.Is(err, entity.ErrInsufficientStock):
			return nil, status.Errorf(codes.FailedPrecondition, "insufficient stock")
		default:
			return nil, status.Errorf(codes.Internal, "%v", err)
		}
	}

	return &emptypb.Empty{}, nil
}
```

Бизнес-логика выглядит следующим образом:

```go
func (s *itemService) AddItem(ctx context.Context, userID int64, sku, count uint32) error {
	_, err := s.productClient.GetProductInfo(ctx, sku)

	if err != nil {
		switch {
		case errors.Is(err, port.ErrProductNotFound):
			return fmt.Errorf("get product info error: %w", entity.ErrProductNotFound)
		default:
			return err
		}
	}

	available, _ := s.lomsClient.GetStocks(ctx, sku)

	if uint64(count) > available {
		return fmt.Errorf("insufficient stock, requested %d, got %d: %w",
			count,
			available,
			entity.ErrInsufficientStock,
		)
	}

	return s.cartRepository.AddItem(ctx, userID, sku, count)
}
```

На текущем этапе мы работаем с in-memory-хранилищем:

```go
type inMemoryRepository struct {
	mx    sync.RWMutex
	items map[int64][]entity.OrderItem
}

func NewInMemoryRepository() *inMemoryRepository {
	return &inMemoryRepository{
		items: make(map[int64][]entity.OrderItem),
	}
}
```

> При внимательном рассмотрении можно заметить, что in-memory хранилище не обеспечивает персистентность данных и полноценные транзакционные гарантии.
> При перезапуске данные теряются. Более того, если выполнение запроса будет прервано, в хранилище может появиться неконсистентное состояние.
> Мы исправим это в следующих заданиях.

## Тестирование

В этом задании вам необходимо сгенерировать моки:

Пример:
```go
//go:generate mockgen -source=cart.go -destination=mocks/cart_mocks.go -package=mocks
type (
	ItemService interface {
		AddItem(ctx context.Context, userID int64, sku, count uint32) error
		DeleteItem(ctx context.Context, userID int64, sku uint32) error
	}

	CartService interface {
		ListCart(ctx context.Context, userID int64) (items []entity.OrderItem, totalPrice uint32, err error)
		ClearCart(ctx context.Context, userID int64) error
		CheckoutCart(ctx context.Context, userID int64) (orderID int64, err error)
	}
)
```

После чего написать юнит-тесты, желательно покрыть тестами всё, в CI проверяется покрытие кода > 60%. Добавление сгенерированных моков в репозиторий считается ошибкой,
необходимо генерировать их (`task generate` запускает `go generate`).

## Локальный запуск

- `task update hw=hw1` — скачивает docker-compose файлы для тестирования и локального запуска
- `task frontend` — запускает frontend, перед выполнением необходимо установить [npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm)
- `task backend` — запускает backend (loms и cart)
- На Windows без WSL многое может не работать


## Особенности реализации
* В этом задании можно не заводить отдельные структуры под слой repository.
* Настоятельно рекомендуется писать комментарии для проверяющих, чтобы обосновать свою точку зрения в выборе
того или иного компромисса (trade-off’а).


## Сдача
* Открыть pull request из ветки `hw1` в ветку `main` **вашего репозитория**, название ветки **важно**
* В описании PR заполнить количество часов, которые вы потратили на это задание
* Не стоит изменять файлы в директории [.github](.github)


## Скрипты
Для запуска скриптов на курсе необходимо установить [go-task](https://taskfile.dev/docs/installation)

`go install github.com/go-task/task/v3/cmd/task@latest`

Перед выполнением задания не забудьте выполнить:

```bash 
task update hw=hw1
```

Запустить линтер:
```bash 
task lint
```

Запустить тесты:
```bash
task test
``` 

Обновить файлы задания
```bash
task update hw=hw1
```

Принудительно обновить файлы задания
```bash
task force-update hw=hw1
```

Запустить генерацию кода:
```bash
task generate
```

Запустить backend в docker:
```bash
task backend
```

Запустить frontend в docker:
```bash
task frontend
```

Скрипты работают на Windows, однако при разработке на этой операционной системе
рекомендуется использовать [WSL](https://learn.microsoft.com/en-us/windows/wsl/install)
